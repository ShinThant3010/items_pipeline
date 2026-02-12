from pydantic import BaseModel, Field


class EmbedDataRequest(BaseModel):
    bigquery_table: str = Field(..., description="BigQuery source table")
    where: str | None = None
    column_list: list[str] | None = None
    gcs_output_prefix: str = Field(..., description="GCS output prefix")
    dimension: int | None = None
    filename: str | None = None
    file_type: str | None = None
