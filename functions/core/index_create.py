from typing import Any

from google.cloud import aiplatform
from google.cloud.aiplatform.matching_engine import matching_engine_index_config

from api.exceptions import PipelineException
from api.schemas.index import IndexCreateRequest
from functions.utils.validators import apply_defaults


def _distance_measure(distance: str) -> matching_engine_index_config.DistanceMeasureType:
    mapping = {
        "DOT_PRODUCT": matching_engine_index_config.DistanceMeasureType.DOT_PRODUCT_DISTANCE,
        "COSINE": matching_engine_index_config.DistanceMeasureType.COSINE_DISTANCE,
        "L2_NORM": matching_engine_index_config.DistanceMeasureType.SQUARED_L2_DISTANCE,
    }
    return mapping.get(distance, matching_engine_index_config.DistanceMeasureType.DOT_PRODUCT_DISTANCE)


def _feature_norm_type(value: str | None) -> matching_engine_index_config.FeatureNormType:
    mapping = {
        "UNIT_L2_NORM": matching_engine_index_config.FeatureNormType.UNIT_L2_NORM,
        "NONE": matching_engine_index_config.FeatureNormType.NONE,
    }
    return mapping.get(value, matching_engine_index_config.FeatureNormType.NONE)


def _create_tree_ah_index(payload: dict[str, Any], project_id: str, region: str) -> dict[str, Any]:
    aiplatform.init(project=project_id, location=region)

    index = aiplatform.MatchingEngineIndex.create_tree_ah_index(
        display_name=payload["display_name"],
        dimensions=payload["dimensions"],
        shard_size=payload.get("shard_size", "SHARD_SIZE_SMALL"),
        distance_measure_type=_distance_measure(payload.get("distance_measure_type", "DOT_PRODUCT")),
        feature_norm_type=_feature_norm_type(payload.get("feature_norm_type", "UNIT_L2_NORM")),
        index_update_method=payload.get("index_update_method", "STREAM_UPDATE"),
        approximate_neighbors_count=payload.get("approximate_neighbors_count", 150),
        leaf_node_embedding_count=payload.get("leaf_node_embedding_count", 1000),
        leaf_nodes_to_search_percent=payload.get("leaf_nodes_to_search_percent", 5),
        description=payload.get("description"),
    )

    return {
        "index_id": index.resource_name,
        "status": "CREATED",
        "request": payload,
    }


def create_index(payload: IndexCreateRequest, config: dict) -> dict:
    defaults = config.get("index_create", {})
    request = apply_defaults(payload, defaults)

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        return _create_tree_ah_index(request, project_id=project_id, region=region)
    except Exception as exc:
        raise PipelineException(f"Failed to create Vertex AI index: {exc}", status_code=500) from exc
