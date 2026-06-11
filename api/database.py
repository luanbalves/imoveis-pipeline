"""
Gerenciamento de conexão com o DuckDB.

Usa um padrão de conexão por request, cada request abre
e fecha sua própria conexão
"""

import duckdb
from contextlib import contextmanager
from pathlib import Path

DB_PATH = str(Path(__file__).parent.parent / "data" / "imoveis_pipeline.duckdb")


@contextmanager
def get_db():
    """Context manager que garante fechamento da conexão."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        yield conn
    finally:
        conn.close()