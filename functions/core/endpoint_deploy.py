from google.cloud import aiplatform

from api.exceptions import PipelineException
from api.schemas.endpoint import EndpointDeployRequest
from functions.utils.validators import apply_defaults


def endpoint_deploy(payload: EndpointDeployRequest, config: dict) -> dict:
    defaults = config.get("endpoint_deploy", {})
    request = apply_defaults(payload, defaults)

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        endpoint_id = request["endpoint_id"]
        index_id = request["index_id"]
        deployed_index_id = request["deployed_index_id"]

        aiplatform.init(project=project_id, location=region)

        endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=endpoint_id)
        index = aiplatform.MatchingEngineIndex(index_name=index_id)

        endpoint.deploy_index(
            index=index,
            deployed_index_id=deployed_index_id,
            machine_type=request.get("machine_type", "e2-standard-2"),
            min_replica_count=request.get("min_replica_count", 1),
            max_replica_count=request.get("max_replica_count", 1),
        )

        return {
            "deployed_index_id": deployed_index_id,
            "endpoint_id": endpoint_id,
            "status": "DEPLOYED",
            "request": request,
        }
    except PipelineException:
        raise
    except Exception as exc:
        raise PipelineException(f"Failed to deploy index: {exc}", status_code=500) from exc
