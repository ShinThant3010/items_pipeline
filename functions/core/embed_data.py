from typing import Any

import numpy as np
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from api.exceptions import PipelineException
from api.schemas.embedding import EmbedDataRequest
from functions.utils.bigquery import query_table
from functions.utils.gcs import write_to_gcs
from functions.utils.validators import apply_defaults


def _l2_normalize(mat: np.ndarray) -> np.ndarray:
    """
    L2-normalize rows of a 2D matrix.

    - Keeps dtype as float32 where possible
    - Avoids division by zero by treating zero-norm rows as norm=1
    """
    if mat.ndim != 2:
        raise ValueError("_l2_normalize expects a 2D array")
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms = np.where(norms == 0.0, 1.0, norms)
    return mat / norms


def _build_text(row: dict[str, Any], text_fields: list[str]) -> str:
    parts = [
        str(row.get(field)).strip()
        for field in text_fields
        if row.get(field) not in (None, "")
    ]
    text = "\n".join(part for part in parts if part)
    return text if text else " "


def _build_metadata(row: dict[str, Any], metadata_fields: list[str]) -> dict[str, Any]:
    if not metadata_fields:
        return row
    return {field: row.get(field) for field in metadata_fields if field in row}


def _embed_texts(
    *,
    project_id: str,
    region: str,
    embedding_model: str,
    output_dimensionality: int,
    texts: list[str],
) -> np.ndarray:
    vertexai.init(project=project_id, location=region)
    model = TextEmbeddingModel.from_pretrained(embedding_model)
    inputs = [
        TextEmbeddingInput(text=text, task_type="RETRIEVAL_DOCUMENT") for text in texts
    ]
    embeddings = model.get_embeddings(
        inputs, output_dimensionality=output_dimensionality
    )
    return _l2_normalize(
        np.asarray([embedding.values for embedding in embeddings], dtype=np.float32)
    )


def embed_data(payload: EmbedDataRequest, config: dict) -> dict:
    defaults = config.get("embed_data", {})
    request = apply_defaults(payload, defaults)

    # Validate config
    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )

    # Main processing - query, embed, write to GCS
    try:
        # Query data from BigQuery
        column_list = request.get("column_list") or defaults.get("column_list")
        rows = query_table(
            request["bigquery_table"],
            request["where"],
            column_list=column_list,
        )
        if not rows:
            raise PipelineException(
                "No rows found for the given bigquery_table/where filter. No file was written to GCS.",
                status_code=400,
            )

        # Prepare embedding parameters
        embedding_model = (
            request.get("embedding_model_name")
            or defaults.get("embedding_model_name")
            or "gemini-embedding-001"
        )
        text_fields = (
            request.get("text_fields")
            or defaults.get("text_fields")
            or ["title", "description", "name"]
        )
        metadata_fields = (
            request.get("metadata_fields") or defaults.get("metadata_fields") or []
        )
        filename = request.get("filename") or defaults.get("filename") or "part-00000"
        file_type = request.get("file_type") or defaults.get("file_type") or "json"
        output_dimensionality = int(request["dimension"])

        texts = [_build_text(row, text_fields=text_fields) for row in rows]

        # Generate embeddings
        vectors = _embed_texts(
            project_id=project_id,
            region=region,
            embedding_model=embedding_model,
            output_dimensionality=output_dimensionality,
            texts=texts,
        )

        # Prepare JSON items for GCS
        json_items: list[dict[str, Any]] = []
        for idx, (row, vector) in enumerate(zip(rows, vectors), start=1):
            datapoint_id = str(
                row.get("id") or row.get("uuid") or row.get("code") or idx
            )
            json_items.append(
                {
                    "id": datapoint_id,
                    "embedding": vector.tolist(),
                    "embedding_metadata": _build_metadata(row, metadata_fields),
                }
            )

        # Write results to GCS
        gcs_uri = write_to_gcs(
            request["gcs_output_prefix"],
            json_items,
            filename=filename,
            file_type=file_type,
        )

        return {
            "status": "EMBEDDED",
            "gcs_output_prefix": request["gcs_output_prefix"],
            "gcs_output_file": gcs_uri,
            "row_count": len(rows),
            "dimension": output_dimensionality,
        }
    except PipelineException:
        raise
    except ValueError as exc:
        raise PipelineException(str(exc), status_code=400) from exc
    except Exception as exc:
        raise PipelineException(
            f"Failed to embed data: {exc}", status_code=500
        ) from exc
