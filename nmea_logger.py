import socket
import csv
import time
import sys
import threading
from datetime import datetime

GPS_HOST = "127.0.0.1"
GPS_PORT = 2947

DEPTH_HOST = "127.0.0.1"
DEPTH_PORT = 10110

CSV_FILE = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Latest values from each source (protected by lock)
lock = threading.Lock()

gps_data = {
    "utc_time": "",
    "latitude": "",
    "longitude": "",
    "speed_knots": "",
    "course": "",
    "fix_quality": "",
    "num_satellites": "",
}

depth_data = {
    "depth_m": "",
    "depth_ft": "",
    "depth_fathoms": "",
}


def parse_lat_lon(raw, direction):
    if not raw or not direction:
        return ""
    try:
        if direction in ("N", "S"):
            degrees = float(raw[:2])
            minutes = float(raw[2:])
        else:
            # Normalize to 5 integer digits before decimal (dddmm.mmmm)
            dot = raw.index(".")
            int_part = raw[:dot]
            if len(int_part) < 5:
                raw = raw.zfill(len(raw) + (5 - len(int_part)))
            degrees = float(raw[:3])
            minutes = float(raw[3:])
        decimal = degrees + minutes / 60.0
        if direction in ("S", "W"):
            decimal = -decimal
        return f"{decimal:.6f}"
    except (ValueError, IndexError):
        return ""


def parse_gga(fields):
    """Parse $xxGGA - GPS fix data."""
    if len(fields) < 10:
        return
    with lock:
        gps_data["utc_time"] = fields[1]
        gps_data["latitude"] = parse_lat_lon(fields[2], fields[3])
        gps_data["longitude"] = parse_lat_lon(fields[4], fields[5])
        gps_data["fix_quality"] = fields[6]
        gps_data["num_satellites"] = fields[7]


def parse_rmc(fields):
    """Parse $xxRMC - Recommended minimum data."""
    if len(fields) < 9:  # FIX: need at least 9 fields to read course at index 8
        return
    with lock:
        gps_data["utc_time"] = fields[1]
        gps_data["latitude"] = parse_lat_lon(fields[3], fields[4])  # FIX: lat is [3]/[4], not [2]/[3]
        gps_data["longitude"] = parse_lat_lon(fields[5], fields[6])  # FIX: lon is [5]/[6], not [4]/[5]
        gps_data["speed_knots"] = fields[7]
        gps_data["course"] = fields[8]


def parse_dbt(fields):
    """Parse $xxDBT - Depth Below Transducer."""
    if len(fields) < 7:
        return
    with lock:
        depth_data["depth_ft"] = fields[1]
        depth_data["depth_m"] = fields[3]
        depth_data["depth_fathoms"] = fields[5]


def parse_dpt(fields):
    """Parse $xxDPT - Depth."""
    if len(fields) < 3:
        return
    with lock:
        depth_data["depth_m"] = fields[1]


def process_sentence(sentence):
    """Parse an NMEA sentence and update state."""
    # FIX: strip whitespace AFTER removing checksum so \r doesn't corrupt fields
    sentence = sentence.strip()
    if not sentence.startswith("$"):
        return False

    if "*" in sentence:
        sentence = sentence[:sentence.index("*")]

    sentence = sentence.strip()  # FIX: strip again after checksum removal

    fields = sentence.split(",")
    if len(fields[0]) < 4:
        return False  # FIX: guard against malformed talker+sentence IDs

    # Handle both $XXYYYY (talker+type) and $YYYY (no talker ID, e.g. $DBT)
    tag = fields[0][1:]  # strip leading $
    if len(tag) == 3:
        msg_type = tag        # e.g. $DBT → "DBT"
    elif len(tag) >= 5:
        msg_type = tag[2:]    # e.g. $IIDBT → "DBT"
    else:
        return False

    if msg_type == "GGA":
        parse_gga(fields)
    elif msg_type == "RMC":
        parse_rmc(fields)
    elif msg_type == "DBT":
        parse_dbt(fields)
    elif msg_type == "DPT":
        parse_dpt(fields)
    else:
        return False
    return True


def tcp_listener(host, port, name):
    """Connect to a TCP NMEA source and process incoming sentences."""
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            print(f"[{name}] Connected to {host}:{port}")

            # gpsd requires this handshake to start sending NMEA sentences
            sock.sendall(b'?WATCH={"enable":true,"nmea":true};\n')

            buf = ""
            while True:
                data = sock.recv(1024)
                if not data:
                    break
                buf += data.decode("ascii", errors="ignore")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    process_sentence(line)
        except ConnectionRefusedError:
            print(f"[{name}] Connection refused on {host}:{port}, retrying in 3s...")
            time.sleep(3)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{name}] Disconnected: {e}, reconnecting in 3s...")
            time.sleep(3)


def main():
    print(f"GPS source:   {GPS_HOST}:{GPS_PORT}")
    print(f"Depth source: {DEPTH_HOST}:{DEPTH_PORT}")
    print(f"Logging to {CSV_FILE}")
    print("Press Ctrl+C to stop.\n")

    gps_thread = threading.Thread(
        target=tcp_listener, args=(GPS_HOST, GPS_PORT, "GPS"), daemon=True
    )
    depth_thread = threading.Thread(
        target=tcp_listener, args=(DEPTH_HOST, DEPTH_PORT, "Depth"), daemon=True
    )
    gps_thread.start()
    depth_thread.start()

    csv_fields = [
        "timestamp",
        "utc_time",
        "latitude",
        "longitude",
        "speed_knots",
        "course",
        "fix_quality",
        "num_satellites",
        "depth_m",
        "depth_ft",
        "depth_fathoms",
    ]

    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

        try:
            while True:
                time.sleep(1.0)

                with lock:
                    row = {
                        "timestamp": datetime.now().isoformat(),
                        **gps_data,
                        **depth_data,
                    }

                writer.writerow(row)
                f.flush()

                print(
                    f"Lat: {row['latitude']:>12s}  "
                    f"Lon: {row['longitude']:>12s}  "
                    f"Depth: {row['depth_m']:>6s} m  "
                    f"Sats: {row['num_satellites']}"
                )

        except KeyboardInterrupt:
            print(f"\nStopped. Log saved to {CSV_FILE}")


if __name__ == "__main__":
    main()
