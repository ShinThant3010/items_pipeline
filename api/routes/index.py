from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.index import IndexCreateRequest
from functions.core.index_create import create_index

router = APIRouter(prefix="/v1")


@router.post("/index/create/", response_model=APIResponse)
def create_index_route(payload: IndexCreateRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = create_index(payload, config)
    return APIResponse(detail="index create request accepted", result=result)
