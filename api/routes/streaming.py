from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.streaming import StreamingDeleteRequest, StreamingUpdateRequest
from functions.core.streaming_delete import streaming_delete
from functions.core.streaming_update import streaming_update

router = APIRouter(prefix="/v1")


@router.post("/streaming/update/", response_model=APIResponse)
def streaming_update_route(payload: StreamingUpdateRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = streaming_update(payload, config)
    return APIResponse(detail="streaming update request accepted", result=result)


@router.post("/streaming/delete/", response_model=APIResponse)
def streaming_delete_route(payload: StreamingDeleteRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = streaming_delete(payload, config)
    return APIResponse(detail="streaming delete request accepted", result=result)
