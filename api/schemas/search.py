from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Restrict(BaseModel):
    namespace: str
    allow: list[str] = Field(default_factory=list)


class SearchRequest(BaseModel):
    endpoint_id: str = Field(..., description="Index endpoint resource name")
    deployed_index_id: str = Field(..., description="Deployed index id")
    query: str | list[float]
    query_type: Literal["vector", "text"] | None = None
    top_k: int | None = None
    restricts: list[Restrict] | None = None

    @model_validator(mode="after")
    def validate_query(self) -> "SearchRequest":
        if self.query_type == "text" and not isinstance(self.query, str):
            raise ValueError("query must be string when query_type=text")
        if self.query_type == "vector" and not isinstance(self.query, list):
            raise ValueError("query must be number vector when query_type=vector")
        return self
