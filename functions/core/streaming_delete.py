from google.cloud import aiplatform

from api.exceptions import PipelineException
from api.schemas.streaming import StreamingDeleteRequest
from functions.utils.validators import apply_defaults


def streaming_delete(payload: StreamingDeleteRequest, config: dict) -> dict:
    request = apply_defaults(payload, {})

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        index_id = request["index_id"]
        ids = [str(item) for item in request.get("datapoint_ids", [])]
        if not ids:
            raise ValueError("datapoint_ids must not be empty")

        aiplatform.init(project=project_id, location=region)
        index = aiplatform.MatchingEngineIndex(index_name=index_id)
        index.remove_datapoints(datapoint_ids=ids)

        return {
            "index_id": index_id,
            "deleted": len(ids),
        }
    except PipelineException:
        raise
    except Exception as exc:
        raise PipelineException(f"Failed to stream delete datapoints: {exc}", status_code=500) from exc
