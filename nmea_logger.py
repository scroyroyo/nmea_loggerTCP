import serial
import csv
import time
import sys
from datetime import datetime

GPS_PORT = "/dev/ttyACM0"
GPS_BAUD = 9600

DEPTH_PORT = "/dev/ttyACM1"
DEPTH_BAUD = 9600

CSV_FILE = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Latest values from each source
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
    """Convert NMEA lat/lon (ddmm.mmmm) to decimal degrees."""
    if not raw or not direction:
        return ""
    try:
        if direction in ("N", "S"):
            degrees = float(raw[:2])
            minutes = float(raw[2:])
        else:
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
    gps_data["utc_time"] = fields[1]
    gps_data["latitude"] = parse_lat_lon(fields[2], fields[3])
    gps_data["longitude"] = parse_lat_lon(fields[4], fields[5])
    gps_data["fix_quality"] = fields[6]
    gps_data["num_satellites"] = fields[7]


def parse_rmc(fields):
    """Parse $xxRMC - Recommended minimum data."""
    if len(fields) < 8:
        return
    gps_data["utc_time"] = fields[1]
    gps_data["latitude"] = parse_lat_lon(fields[2], fields[3])
    gps_data["longitude"] = parse_lat_lon(fields[4], fields[5])
    gps_data["speed_knots"] = fields[7] if len(fields) > 7 else ""
    gps_data["course"] = fields[8] if len(fields) > 8 else ""


def parse_dbt(fields):
    """Parse $xxDBT - Depth Below Transducer."""
    if len(fields) < 7:
        return
    depth_data["depth_ft"] = fields[1]
    depth_data["depth_m"] = fields[3]
    depth_data["depth_fathoms"] = fields[5]


def parse_dpt(fields):
    """Parse $xxDPT - Depth."""
    if len(fields) < 3:
        return
    depth_data["depth_m"] = fields[1]


def process_sentence(sentence):
    """Parse an NMEA sentence and update state."""
    sentence = sentence.strip()
    if not sentence.startswith("$"):
        return False

    # Strip checksum
    if "*" in sentence:
        sentence = sentence[:sentence.index("*")]

    fields = sentence.split(",")
    msg_type = fields[0][3:]  # strip $XX prefix

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


def main():
    print(f"Opening GPS on {GPS_PORT} @ {GPS_BAUD}")
    print(f"Opening Depth on {DEPTH_PORT} @ {DEPTH_BAUD}")
    print(f"Logging to {CSV_FILE}")
    print("Press Ctrl+C to stop.\n")

    try:
        gps_serial = serial.Serial(GPS_PORT, GPS_BAUD, timeout=0.1)
        depth_serial = serial.Serial(DEPTH_PORT, DEPTH_BAUD, timeout=0.1)
    except serial.SerialException as e:
        print(f"Failed to open serial port: {e}")
        sys.exit(1)

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

        last_log_time = 0
        log_interval = 1.0  # log once per second

        try:
            while True:
                # Read GPS
                try:
                    line = gps_serial.readline().decode("ascii", errors="ignore")
                    if line:
                        process_sentence(line)
                except serial.SerialException:
                    pass

                # Read Depth
                try:
                    line = depth_serial.readline().decode("ascii", errors="ignore")
                    if line:
                        process_sentence(line)
                except serial.SerialException:
                    pass

                # Log at interval
                now = time.time()
                if now - last_log_time >= log_interval:
                    row = {
                        "timestamp": datetime.now().isoformat(),
                        **gps_data,
                        **depth_data,
                    }
                    writer.writerow(row)
                    f.flush()
                    last_log_time = now

                    print(
                        f"Lat: {gps_data['latitude']:>12s}  "
                        f"Lon: {gps_data['longitude']:>12s}  "
                        f"Depth: {depth_data['depth_m']:>6s} m  "
                        f"Sats: {gps_data['num_satellites']}"
                    )

        except KeyboardInterrupt:
            print(f"\nStopped. Log saved to {CSV_FILE}")
        finally:
            gps_serial.close()
            depth_serial.close()


if __name__ == "__main__":
    main()
