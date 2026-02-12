from pydantic import BaseModel, Field


class EndpointCreateRequest(BaseModel):
    display_name: str = Field(..., description="Endpoint display name")
    public_endpoint_enabled: bool | None = None


class EndpointDeployRequest(BaseModel):
    endpoint_id: str = Field(..., description="Index endpoint resource name")
    index_id: str = Field(..., description="Index resource name")
    deployed_index_id: str = Field(..., description="Deployment id")
    machine_type: str | None = None
    min_replica_count: int | None = None
    max_replica_count: int | None = None
