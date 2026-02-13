from pydantic import BaseModel, Field


class EmbedDataRequest(BaseModel):
    bigquery_table: str = Field(..., description="BigQuery source table")
    where: str | None = None
    col_to_embed: list[str] | None = None
    restrict_columns: list[str] | None = None
    numeric_restricts_columns: list[str] | None = None
    gcs_output_prefix: str = Field(..., description="GCS output prefix")
    dimension: int | None = None
    filename: str | None = None
    file_type: str | None = None


class EmbedTextRequest(BaseModel):
    texts: list[str] = Field(..., min_length=1, description="Input texts to embed")
    gcs_output_prefix: str = Field(..., description="GCS output prefix")
    dimension: int | None = None
    filename: str | None = None
    file_type: str | None = None
    embedding_model_name: str | None = None
