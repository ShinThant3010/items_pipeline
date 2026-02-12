from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.search import SearchRequest
from functions.core.search import search

router = APIRouter(prefix="/v1")


@router.post("/search", response_model=APIResponse)
def search_route(payload: SearchRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = search(payload, config)
    return APIResponse(detail="search request completed", result=result)
