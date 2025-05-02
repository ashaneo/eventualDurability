import os, json, time
from .logger  import write_log
from .catalog import load_schema, save_schema

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")
os.makedirs(DATA, exist_ok=True)

# ───────────────────────────────────────────────────────── create
def create_table(name, columns, pk):
    sch = load_schema()
    if name in sch["tables"]:
        print(f"⚠️  Table '{name}' exists."); return
    sch["tables"][name] = {"columns": columns, "primary_key": pk}
    save_schema(sch)
    open(os.path.join(DATA, f"{name}.dat"), "a").close()
    write_log(f"CREATE_TABLE {name} {json.dumps(columns)} PK {pk}")
    print(f"✅ Created '{name}'.")

# ───────────────────────────────────────────────────────── drop
def drop_table(name):
    sch = load_schema()
    if name not in sch["tables"]:
        print("⚠️  No such table."); return
    del sch["tables"][name]; save_schema(sch)
    path = os.path.join(DATA, f"{name}.dat")
    if os.path.exists(path): os.remove(path)
    write_log(f"DROP_TABLE {name}")
    print(f"🗑️  Dropped '{name}'.")

# ───────────────────────────────────────────────────────── insert
def insert(name, row: dict):
    sch = load_schema()
    if name not in sch["tables"]: raise ValueError("table missing")
    path = os.path.join(DATA, f"{name}.dat")
    txn  = int(time.time()*1000)
    write_log(f"BEGIN {txn}")
    write_log(f"INSERT {txn} {name} {json.dumps(row)}")
    write_log(f"COMMIT {txn}")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row)+"\n"); f.flush(); os.fsync(f.fileno())
    print("➕ inserted.")

# ───────────────────────────────────────────────────────── delete by PK
def delete(name, pk_val):
    sch = load_schema()
    if name not in sch["tables"]: raise ValueError("table missing")
    pk = sch["tables"][name]["primary_key"]
    inf = os.path.join(DATA, f"{name}.dat"); tmp = inf+".tmp"
    removed=0
    with open(inf, "r", encoding="utf-8") as src, \
         open(tmp, "w", encoding="utf-8") as dst:
        for line in src:
            row = json.loads(line)
            if row.get(pk)==pk_val: removed+=1
            else: dst.write(line)
        dst.flush(); os.fsync(dst.fileno())
    os.replace(tmp, inf)
    write_log(f"DELETE {name} PK {pk_val} rows {removed}")
    print(f"🗑️  deleted {removed}.")
