from typing import Any

from google.cloud import aiplatform
from google.cloud.aiplatform.matching_engine.matching_engine_index_endpoint import Namespace
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from api.exceptions import PipelineException
from api.schemas.search import SearchRequest
from functions.utils.validators import apply_defaults


def _build_namespace_filters(restricts: list[dict[str, Any]] | None) -> list[Namespace]:
    filters: list[Namespace] = []
    for item in restricts or []:
        namespace = item.get("namespace") or item.get("name")
        if not namespace:
            continue
        allow = item.get("allow") or item.get("allow_list") or []
        deny = item.get("deny") or item.get("deny_list") or []
        filters.append(Namespace(namespace, list(allow), list(deny)))
    return filters


def _extract_neighbor(neighbor: Any) -> dict[str, Any]:
    datapoint = getattr(neighbor, "datapoint", None)
    if datapoint is not None:
        neighbor_id = getattr(datapoint, "datapoint_id", None) or getattr(datapoint, "id", None)
        metadata = getattr(datapoint, "embedding_metadata", None) or getattr(datapoint, "metadata", None)
    else:
        neighbor_id = getattr(neighbor, "id", None)
        metadata = None

    score = getattr(neighbor, "distance", None)
    if score is None:
        score = getattr(neighbor, "score", None)

    return {
        "id": neighbor_id,
        "score": score,
        "metadata": metadata,
    }


def search(payload: SearchRequest, config: dict) -> dict:
    defaults = config.get("search", {})
    request = apply_defaults(payload, defaults)
    request["restricts"] = request.get("restricts") or []

    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    try:
        endpoint_id = request["endpoint_id"]
        deployed_index_id = request["deployed_index_id"]
        query_type = (request.get("query_type") or "vector").lower()
        query = request["query"]
        top_k = int(request.get("top_k", 10))
        restricts = request.get("restricts")

        if query_type == "text":
            if not isinstance(query, str):
                raise ValueError("query must be a string when query_type is 'text'")
            embedding_model = (
                request.get("embedding_model_name")
                or defaults.get("embedding_model_name")
                or config.get("embed_data", {}).get("embedding_model_name")
                or "text-embedding-005"
            )
            output_dimensionality = int(
                request.get("dimension")
                or defaults.get("dimension")
                or config.get("embed_data", {}).get("dimension")
                or 768
            )
            vertexai.init(project=project_id, location=region)
            model = TextEmbeddingModel.from_pretrained(embedding_model)
            embedding = model.get_embeddings(
                [TextEmbeddingInput(text=query, task_type="RETRIEVAL_QUERY")],
                output_dimensionality=output_dimensionality,
            )[0]
            embedding_values = [float(v) for v in embedding.values]
        elif query_type == "vector":
            if not isinstance(query, list) or not all(isinstance(v, (float, int)) for v in query):
                raise ValueError("query must be a list of numbers when query_type is 'vector'")
            embedding_values = [float(v) for v in query]
        else:
            raise ValueError("query_type must be 'text' or 'vector'")

        aiplatform.init(project=project_id, location=region)
        endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=endpoint_id)
        filters = _build_namespace_filters(restricts)
        neighbors = endpoint.find_neighbors(
            deployed_index_id=deployed_index_id,
            queries=[embedding_values],
            num_neighbors=top_k,
            return_full_datapoint=True,
            filter=filters or None,
        )

        results = []
        if neighbors:
            results = [_extract_neighbor(n) for n in neighbors[0]]

        return {
            "query": query,
            "query_type": query_type,
            "num_recommendations": len(results),
            "results": results,
        }
    except PipelineException:
        raise
    except Exception as exc:
        raise PipelineException(f"Failed to search index: {exc}", status_code=500) from exc
