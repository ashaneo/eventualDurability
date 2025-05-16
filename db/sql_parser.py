from sqlglot import parse_one, exp

def parse(sql: str) -> dict:
    print("PARSER WORKING")
    """Return a clean dict AST for the subset we care about."""
    root = parse_one(sql, error_level='raise')   # raises on bad SQL

    if isinstance(root, exp.Create):
        cols   = [c.name for c in root.expressions]
        pk_col = root.properties.get("primaryKey").this
        return {"type": "CREATE", "table": root.this.name, "columns": cols, "pk": pk_col}

    if isinstance(root, exp.Insert):
        values = [v.eval() for v in root.expressions]   # <sqlglot>=18 supports .eval()
        return {"type": "INSERT", "table": root.this.name, "values": values}

    if isinstance(root, exp.Delete):
        pk_val = root.args["where"].this.right.eval()
        return {"type": "DELETE", "table": root.this.name, "value": pk_val}

    if isinstance(root, exp.Drop):
        return {"type": "DROP", "table": root.this.name}

    if isinstance(root, exp.Select):
        if root.args.get("from") and not root.args.get("where"):
            return {"type": "SELECT_ALL", "table": root.args["from"].expressions[0].this}
    raise NotImplementedError("unsupported SQL")
