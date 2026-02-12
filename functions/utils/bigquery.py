from typing import Any

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


def _select_clause(column_list: list[str] | None) -> str:
    if not column_list:
        return "*"
    cols = [col.strip() for col in column_list if col and col.strip()]
    if not cols:
        return "*"
    return ", ".join(f"`{col}`" for col in cols)


def query_table(
    table: str, where_clause: str, column_list: list[str] | None = None
) -> list[dict[str, Any]]:
    """
    Query a BigQuery table with a WHERE clause and return rows as dictionaries.
    """
    client = bigquery.Client()
    select_columns = _select_clause(column_list)
    query = f"SELECT {select_columns} FROM `{table}` WHERE {where_clause}"
    try:
        return [dict(row.items()) for row in client.query(query)]
    except BadRequest as exc:
        raise ValueError(str(exc)) from exc
