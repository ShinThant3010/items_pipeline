from pydantic import BaseModel, Field


class IndexCreateRequest(BaseModel):
    display_name: str = Field(..., description="Index display name")
    description: str | None = None
    dimensions: int | None = None
    shard_size: str | None = None
    distance_measure_type: str | None = None
    feature_norm_type: str | None = None
    index_update_method: str | None = None
    approximate_neighbors_count: int | None = None
    leaf_node_embedding_count: int | None = None
    leaf_nodes_to_search_percent: int | None = None
