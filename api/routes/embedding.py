from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.embedding import EmbedDataRequest, EmbedTextRequest
from functions.core.embed_data import embed_data, embed_text

router = APIRouter(prefix="/v1")


@router.post("/embed_data/", response_model=APIResponse)
def embed_data_route(payload: EmbedDataRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = embed_data(payload, config)
    return APIResponse(detail="embed data request accepted", result=result)


@router.post("/embed_text/", response_model=APIResponse)
def embed_text_route(payload: EmbedTextRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = embed_text(payload, config)
    return APIResponse(detail="embed text request accepted", result=result)
