# db/parser.py  – a micro SQL subset parser
import re, ast, shlex

CREATE = re.compile(r"CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)\s+PRIMARY\s+KEY\s+(\w+);?", re.I)
INSERT = re.compile(r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\);?", re.I)
DELETE = re.compile(r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+);?", re.I)
DROP   = re.compile(r"DROP\s+TABLE\s+(\w+);?", re.I)
SELECT = re.compile(r"SELECT\s+\*\s+FROM\s+(\w+);?", re.I)

def parse(sql: str) -> dict|None:
    sql = sql.strip()
    if m := CREATE.fullmatch(sql):
        tbl, cols, pk = m.groups()
        return {"type": "CREATE", "table": tbl,
                "columns": [c.strip() for c in cols.split(",")],
                "pk": pk}
    if m := INSERT.fullmatch(sql):
        tbl, values = m.groups()
        vals = [ast.literal_eval(v.strip()) for v in shlex.split(values, posix=False)]
        return {"type": "INSERT", "table": tbl, "values": vals}
    if m := DELETE.fullmatch(sql):
        tbl, field, val = m.groups()
        return {"type": "DELETE", "table": tbl,
                "field": field, "value": ast.literal_eval(val)}
    if m := DROP.fullmatch(sql):
        return {"type": "DROP", "table": m.group(1)}
    if m := SELECT.fullmatch(sql):
        return {"type": "SELECT_ALL", "table": m.group(1)}
    return None
