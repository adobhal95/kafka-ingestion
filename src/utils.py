import sys
import datetime
import os
import signal

# --- High-Watermark (Last Read Timestamp) Management ---
LAST_UPDATE_FILE = "last_update.txt"


def get_last_successful_timestamp() -> datetime:
    """Reads the last successfully processed timestamp from a file."""
    if not os.path.exists(LAST_UPDATE_FILE):
        return datetime.datetime(1900, 1, 1)  # Fallback to a very old date
    with open(file=LAST_UPDATE_FILE, mode='r') as file:
        content = file.read().strip()
        if content:
            return datetime.datetime.strptime(content, "%Y-%m-%d %H:%M:%S.%f")
        return datetime.datetime(1900, 1, 1)


def set_last_successful_timestamp(timestamp: datetime):
    """Writes the last successfully processed timestamp to a file."""
    with open(file=LAST_UPDATE_FILE, mode='w') as file:
        file.write(timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"))


# --- Producer Delivery Report Callback ---
def delivery_callback(err, msg):
    """Callback function for message delivery."""
    if err is not None:
        print(f"Message delivery failed for key {msg.key().decode('utf-8') if msg.key() else 'N/A'}: {err}",
              file=sys.stderr)
    else:
        print(
            f"Message record {msg.key().decode('utf-8') if msg.key() else 'N/A'} successfully produced to Topic:{msg.topic()} Partition:[{msg.partition()}] at Offset:{msg.offset()}")


running = True  # Global flag for graceful shutdown


def signal_handler(signum, frame):
    global running
    print("\nCtrl+C pressed. Initiating graceful shutdown...")
    running = False
