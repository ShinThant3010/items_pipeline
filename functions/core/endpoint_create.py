from google.cloud import aiplatform

from api.exceptions import PipelineException
from api.schemas.endpoint import EndpointCreateRequest
from functions.utils.validators import apply_defaults


def endpoint_create(payload: EndpointCreateRequest, config: dict) -> dict:
    defaults = config.get("endpoint_create", {})
    request = apply_defaults(payload, defaults)

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        aiplatform.init(project=project_id, location=region)
        endpoint = aiplatform.MatchingEngineIndexEndpoint.create(
            display_name=request["display_name"],
            description=request.get("description"),
            public_endpoint_enabled=request.get("public_endpoint_enabled", True),
        )
        return {
            "endpoint_id": endpoint.resource_name,
            "status": "CREATED",
            "request": request,
        }
    except PipelineException:
        raise
    except Exception as exc:
        raise PipelineException(f"Failed to create endpoint: {exc}", status_code=500) from exc
