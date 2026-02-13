from datetime import datetime
from typing import Any

import numpy as np
import vertexai
from api.exceptions import PipelineException
from api.schemas.embedding import EmbedDataRequest, EmbedTextRequest
from functions.utils.bigquery import query_table
from functions.utils.gcs import write_to_gcs
from functions.utils.validators import apply_defaults
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel


def _l2_normalize(mat: np.ndarray) -> np.ndarray:
    if mat.ndim != 2:
        raise ValueError("_l2_normalize expects a 2D array")
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms = np.where(norms == 0.0, 1.0, norms)
    return mat / norms


def _build_text(row: dict[str, Any], column_list: list[str]) -> str:
    parts = [
        str(row.get(field)).strip()
        for field in column_list
        if row.get(field) not in (None, "")
    ]
    text = "\n".join(part for part in parts if part)
    return text if text else " "


def _to_epoch_seconds(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return int(text)

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(text, fmt).timestamp())
        except ValueError:
            continue

    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _build_restricts(
    row: dict[str, Any], restrict_columns: list[str]
) -> list[dict[str, Any]]:
    restricts: list[dict[str, Any]] = []
    for column in restrict_columns:
        value = row.get(column)
        if value is None or value == "":
            continue
        if isinstance(value, (list, tuple)):
            allow = [str(v) for v in value if v not in (None, "")]
        else:
            allow = [str(value)]
        if allow:
            restricts.append({"namespace": column, "allow": allow})
    return restricts


def _build_numeric_restricts(
    row: dict[str, Any], numeric_restricts_columns: list[str]
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for column in numeric_restricts_columns:
        raw = row.get(column)
        if raw is None or raw == "":
            continue

        if isinstance(raw, float):
            items.append({"namespace": column, "value_float": float(raw)})
            continue

        parsed = _to_epoch_seconds(raw)
        if parsed is not None:
            items.append({"namespace": column, "value_int": int(parsed)})
    return items


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


def _require_project_config(config: dict) -> tuple[str, str]:
    project_id = config.get("project_id")
    region = config.get("region")
    if not project_id or not region:
        raise PipelineException(
            "Missing `project_id` or `region` in functions/parameters/config.yaml",
            status_code=500,
        )
    return project_id, region


def embed_text(payload: EmbedTextRequest, config: dict) -> dict:
    defaults = config.get("embed_text", {})
    request = apply_defaults(payload, defaults)

    project_id, region = _require_project_config(config)

    try:
        texts = [str(text).strip() for text in request["texts"] if str(text).strip()]
        if not texts:
            raise PipelineException("texts must not be empty", status_code=400)

        embedding_model = (
            request.get("embedding_model_name")
            or defaults.get("embedding_model_name")
            or "gemini-embedding-001"
        )
        filename = request.get("filename") or defaults.get("filename") or "part-00000"
        file_type = request.get("file_type") or defaults.get("file_type") or "json"
        output_dimensionality = int(
            request.get("dimension") or defaults.get("dimension") or 768
        )

        vectors = _embed_texts(
            project_id=project_id,
            region=region,
            embedding_model=embedding_model,
            output_dimensionality=output_dimensionality,
            texts=texts,
        )

        items = [{"embedding": vector.tolist()} for vector in vectors]

        gcs_uri = write_to_gcs(
            request["gcs_output_prefix"],
            items,
            filename=filename,
            file_type=file_type,
        )

        return {
            "status": "EMBEDDED",
            "mode": "text",
            "gcs_output_prefix": request["gcs_output_prefix"],
            "gcs_output_file": gcs_uri,
            "row_count": len(texts),
            "dimension": output_dimensionality,
        }
    except PipelineException:
        raise
    except ValueError as exc:
        raise PipelineException(str(exc), status_code=400) from exc
    except Exception as exc:
        raise PipelineException(
            f"Failed to embed text: {exc}", status_code=500
        ) from exc


def embed_data(payload: EmbedDataRequest, config: dict) -> dict:
    defaults = config.get("embed_data", {})
    request = apply_defaults(payload, defaults)

    project_id, region = _require_project_config(config)

    try:
        rows = query_table(request["bigquery_table"], request["where"])
        if not rows:
            raise PipelineException(
                "No rows found for the given bigquery_table/where filter. No file was written to GCS.",
                status_code=400,
            )

        embedding_model = (
            request.get("embedding_model_name")
            or defaults.get("embedding_model_name")
            or "gemini-embedding-001"
        )
        restrict_columns = (
            request.get("restrict_columns") or defaults.get("restrict_columns") or []
        )
        numeric_restricts_columns = (
            request.get("numeric_restricts_columns")
            or defaults.get("numeric_restricts_columns")
            or []
        )
        filename = request.get("filename") or defaults.get("filename") or "part-00000"
        file_type = request.get("file_type") or defaults.get("file_type") or "json"
        output_dimensionality = int(request["dimension"])

        text_column_list = (
            request.get("col_to_embed") or defaults.get("col_to_embed") or []
        )
        if not text_column_list:
            text_column_list = [
                key for key in rows[0].keys() if key not in {"id", "uuid", "code"}
            ]
        texts = [_build_text(row, column_list=text_column_list) for row in rows]
        vectors = _embed_texts(
            project_id=project_id,
            region=region,
            embedding_model=embedding_model,
            output_dimensionality=output_dimensionality,
            texts=texts,
        )

        items: list[dict[str, Any]] = []
        for idx, (row, vector) in enumerate(zip(rows, vectors), start=1):
            datapoint_id = str(
                row.get("id") or row.get("uuid") or row.get("code") or idx
            )
            item: dict[str, Any] = {
                "id": datapoint_id,
                "embedding": vector.tolist(),
                "restricts": _build_restricts(row, restrict_columns),
                "numeric_restricts": _build_numeric_restricts(
                    row, numeric_restricts_columns
                ),
            }
            items.append(item)

        gcs_uri = write_to_gcs(
            request["gcs_output_prefix"],
            items,
            filename=filename,
            file_type=file_type,
        )

        return {
            "status": "EMBEDDED",
            "mode": "vertex_index_datapoints",
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
