# import re, ast, shlex, sys
# from db.storage  import create_table, insert, delete, drop_table
# from db.catalog  import load_schema
# from db.recovery import recover
# from pathlib import Path
# from rich import print

# recover()

# CREATE = re.compile(r"CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)\s+PRIMARY\s+KEY\s+(\w+);?", re.I)
# INSERT = re.compile(r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\);?", re.I)
# DELETE = re.compile(r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(.+);?", re.I)
# DROP   = re.compile(r"DROP\s+TABLE\s+(\w+);?", re.I)
# SELECT = re.compile(r"SELECT\s+\*\s+FROM\s+(\w+);?", re.I)

# DATA_PATH = Path("data")

# def pval(tok:str):
#     tok=tok.strip()
#     if tok.startswith(("'",'"')): return tok[1:-1]
#     return int(tok) if tok.isdigit() else tok

# def run(sql:str):
#     sql=sql.strip()
#     if m:=CREATE.fullmatch(sql):
#         t,c,pk=m.groups(); cols=[x.strip() for x in c.split(',')]
#         create_table(t,cols,pk)
#     elif m:=INSERT.fullmatch(sql):
#         table,vals=m.groups()
#         vals=[pval(v) for v in re.split(r",(?![^']*')", vals)]
#         sch=load_schema(); cols=sch["tables"][table]["columns"]
#         insert(table, dict(zip(cols, vals)))
#     elif m:=DELETE.fullmatch(sql):
#         table, field, val=m.groups(); val=pval(val)
#         pk=load_schema()["tables"][table]["primary_key"]
#         if field!=pk: print("[red]DELETE allowed only by PK[/red]");return
#         delete(table,val)
#     elif m:=DROP.fullmatch(sql):
#         drop_table(m.group(1))
#     elif m:=SELECT.fullmatch(sql):
#         tbl=m.group(1); fp=DATA_PATH/f"{tbl}.dat"
#         if fp.exists(): print(fp.read_text())
#         else: print("[yellow]table not found[/yellow]")
#     else:
#         print("[red]Unsupported / bad SQL[/red]")

# def repl():
#     print("[bold green]smallSQL ready (type quit to exit)[/bold green]")
#     while True:
#         try:
#             cmd=input("sql> ").strip()
#             if cmd.lower() in ("quit","exit"): break
#             if cmd: run(cmd)
#         except KeyboardInterrupt:
#             break
#         except Exception as e:
#             print(f"[red]Error:[/red] {e}")

# if __name__=="__main__": repl()

from db.parser   import parse
from db.storage  import create_table, insert, delete, drop_table
from db.catalog  import load_schema
from db.recovery import recover
from pathlib import Path
from rich import print

def execute(ast):
    if ast["type"] == "CREATE":
        create_table(ast["table"], ast["columns"], ast["pk"])
    elif ast["type"] == "INSERT":
        cols = load_schema()["tables"][ast["table"]]["columns"]
        insert(ast["table"], dict(zip(cols, ast["values"])))
    elif ast["type"] == "DELETE":
        delete(ast["table"], ast["value"])
    elif ast["type"] == "DROP":
        drop_table(ast["table"])
    elif ast["type"] == "SELECT_ALL":
        path = Path("data") / f"{ast['table']}.dat"
        print(path.read_text() if path.exists() else "[yellow]table not found[/yellow]")
    else:
        print("[red]Unsupported SQL[/red]")

def repl():
    recover()
    print("[green]smallSQL ready without eventual DB[/green]")
    while True:
        try:
            line = input("sql> ").strip()
            if line.lower() in ("quit","exit"): break
            ast = parse(line)
            if ast: execute(ast)
            else:   print("[red]Syntax error[/red]")
        except Exception as e:
            print("[red]Error:[/red]", e)

if __name__ == "__main__":
    repl()
