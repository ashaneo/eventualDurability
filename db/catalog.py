import json, os
BASE   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA = os.path.join(BASE, "metadata", "schema.json")
os.makedirs(os.path.dirname(SCHEMA), exist_ok=True)

def _init_schema():
    with open(SCHEMA, "w", encoding="utf-8") as f:
        json.dump({"tables": {}}, f)

def load_schema() -> dict:
    if not os.path.exists(SCHEMA):
        _init_schema()

    # try reading; if corrupted or empty -> reset
    try:
        with open(SCHEMA, encoding="utf-8") as f:
            print("schema loaded")
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        print("[yellow]schema.json empty or corrupt – re-initialising[/yellow]")
        _init_schema()
        with open(SCHEMA, encoding="utf-8") as f:
            return json.load(f)

def save_schema(schema: dict):
    with open(SCHEMA, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=4)
        f.flush(); os.fsync(f.fileno())
