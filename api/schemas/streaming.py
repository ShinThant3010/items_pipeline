from typing import Literal

from pydantic import BaseModel, Field


class StreamingUpdateRequest(BaseModel):
    index_id: str = Field(..., description="Vertex index resource name")
    datapoints_source: Literal["gcs"] | None = None
    datapoints_gcs_prefix: str = Field(..., description="GCS prefix for datapoints")


class StreamingDeleteRequest(BaseModel):
    index_id: str = Field(..., description="Vertex index resource name")
    datapoint_ids: list[str] = Field(..., min_length=1)
