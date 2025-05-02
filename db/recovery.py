import os, json, re
from .catalog import load_schema, save_schema
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG  = os.path.join(BASE, "logs", "wal.log")
DATA = os.path.join(BASE, "data")

def recover():
    if not os.path.exists(LOG): return
    committed=set()
    with open(LOG,encoding="utf-8") as wal:
        for l in wal:
            if " COMMIT " in l:
                committed.add(l.split()[3])
    sch = load_schema()
    with open(LOG,encoding="utf-8") as wal:
        for l in wal:
            _, entry = l.strip().split(": ",1)
            parts=entry.split(maxsplit=4)
            op=parts[0]
            if op=="CREATE_TABLE":
                _, tbl, cols_json, _, pk = parts
                if tbl not in sch["tables"]:
                    sch["tables"][tbl]={"columns":json.loads(cols_json),"primary_key":pk}
                    open(os.path.join(DATA,f"{tbl}.dat"),"a").close()
            elif op=="INSERT":
                txn,tbl,row_json=parts[1],parts[2],parts[3]
                if txn in committed:
                    with open(os.path.join(DATA,f"{tbl}.dat"),"a") as f: f.write(row_json+"\n")
    save_schema(sch)
    print("🔄 recovery done.")
