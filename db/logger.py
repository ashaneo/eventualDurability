import os, time
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG   = os.path.join(BASE, "logs", "wal.log")
os.makedirs(os.path.dirname(LOG), exist_ok=True)

def write_log(entry: str):
    ts = time.time()
    with open(LOG, "a", encoding="utf-8") as wal:
        wal.write(f"{ts}: {entry}\n")
        wal.flush()
        os.fsync(wal.fileno())      # immediate durability
