from typing import Any

from google.cloud import aiplatform
from google.cloud.aiplatform_v1.types import index as gca_index
from google.protobuf.struct_pb2 import Struct

from api.exceptions import PipelineException
from api.schemas.streaming import StreamingUpdateRequest
from functions.utils.gcs import load_data_from_gcs_prefix
from functions.utils.validators import apply_defaults


def _struct_from_dict(data: dict[str, Any] | None) -> Struct | None:
    if not data:
        return None
    struct = Struct()
    struct.update(data)
    return struct


def _build_index_datapoints(items: list[dict[str, Any]]) -> list[gca_index.IndexDatapoint]:
    datapoints: list[gca_index.IndexDatapoint] = []
    for item in items:
        embedding_metadata = _struct_from_dict(item.get("embedding_metadata"))
        restricts = [
            gca_index.IndexDatapoint.Restriction(
                namespace=restrict.get("namespace", ""),
                allow_list=restrict.get("allow") or restrict.get("allow_list") or [],
                deny_list=restrict.get("deny") or restrict.get("deny_list") or [],
            )
            for restrict in item.get("restricts", []) or []
        ]
        numeric_restricts = [
            gca_index.IndexDatapoint.NumericRestriction(**restrict)
            for restrict in item.get("numeric_restricts", []) or []
        ]

        datapoints.append(
            gca_index.IndexDatapoint(
                datapoint_id=str(item.get("id")),
                feature_vector=item.get("embedding", []),
                restricts=restricts,
                numeric_restricts=numeric_restricts,
                embedding_metadata=embedding_metadata,
            )
        )
    return datapoints


def streaming_update(payload: StreamingUpdateRequest, config: dict) -> dict:
    defaults = config.get("streaming_update", {})
    request = apply_defaults(payload, defaults)

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        index_id = request["index_id"]
        datapoints_source = request.get("datapoints_source", "gcs")
        if datapoints_source != "gcs":
            raise ValueError("Only datapoints_source='gcs' is supported")
        datapoints_gcs_prefix = request.get("datapoints_gcs_prefix")
        if not datapoints_gcs_prefix:
            raise ValueError("datapoints_gcs_prefix is required")

        items = load_data_from_gcs_prefix(
            datapoints_gcs_prefix,
            field_name="datapoints_gcs_prefix",
            file_type="json",
        )
        datapoints = _build_index_datapoints(items)

        aiplatform.init(project=project_id, location=region)
        index = aiplatform.MatchingEngineIndex(index_name=index_id)
        index.upsert_datapoints(datapoints=datapoints)

        return {
            "index_id": index_id,
            "upserted": len(datapoints),
            "datapoints_source": datapoints_source,
            "datapoints_gcs_prefix": datapoints_gcs_prefix,
        }
    except PipelineException:
        raise
    except Exception as exc:
        raise PipelineException(f"Failed to stream update datapoints: {exc}", status_code=500) from exc
