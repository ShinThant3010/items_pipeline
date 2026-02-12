from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.embedding import EmbedDataRequest
from functions.core.embed_data import embed_data

router = APIRouter(prefix="/v1")


@router.post("/embed_data/", response_model=APIResponse)
def embed_data_route(payload: EmbedDataRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = embed_data(payload, config)
    return APIResponse(detail="embed data request accepted", result=result)
