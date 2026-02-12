from fastapi import APIRouter, Depends

from api.deps import get_config
from api.schemas.common import APIResponse
from api.schemas.endpoint import EndpointCreateRequest, EndpointDeployRequest
from functions.core.endpoint_create import endpoint_create
from functions.core.endpoint_deploy import endpoint_deploy

router = APIRouter(prefix="/v1")


@router.post("/endpoint/create/", response_model=APIResponse)
def endpoint_create_route(payload: EndpointCreateRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = endpoint_create(payload, config)
    return APIResponse(detail="endpoint create request accepted", result=result)


@router.post("/endpoint/deploy/", response_model=APIResponse)
def endpoint_deploy_route(payload: EndpointDeployRequest, config: dict = Depends(get_config)) -> APIResponse:
    result = endpoint_deploy(payload, config)
    return APIResponse(detail="endpoint deploy request accepted", result=result)
