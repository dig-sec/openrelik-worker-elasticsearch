import json
import os
from typing import Any

from elasticsearch import Elasticsearch
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

TASK_NAME = "openrelik-worker-elasticsearch.tasks.export"

TASK_METADATA = {
    "display_name": "Elasticsearch Export",
    "description": "Export worker output results into an Elasticsearch index.",
    "task_config": [
        {
            "name": "index_name",
            "label": "Elasticsearch index name",
            "description": "Index where documents will be stored.",
            "type": "text",
            "required": True,
        },
        {
            "name": "id_field",
            "label": "Document ID field",
            "description": "Optional JSON field to use as Elasticsearch _id.",
            "type": "text",
            "required": False,
        },
        {
            "name": "parse_json_lines",
            "label": "Parse input as JSON lines",
            "description": "If unchecked, each line is indexed as plain text.",
            "type": "checkbox",
            "required": False,
            "default_value": True,
        },
        {
            "name": "elasticsearch_url",
            "label": "Elasticsearch URL",
            "description": (
                "Optional Elasticsearch instance URL " "(overrides ELASTICSEARCH_URL)."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "auth_mode",
            "label": "Auth mode",
            "description": (
                "Optional auth mode: none | api_key | basic. If empty, "
                "worker auto-detects from provided credentials."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "api_key",
            "label": "Elasticsearch API key",
            "description": (
                "Optional API key for auth_mode=api_key "
                "(overrides ELASTICSEARCH_API_KEY)."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "username",
            "label": "Elasticsearch username",
            "description": (
                "Optional username for auth_mode=basic "
                "(overrides ELASTICSEARCH_USERNAME)."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "password",
            "label": "Elasticsearch password",
            "description": (
                "Optional password for auth_mode=basic "
                "(overrides ELASTICSEARCH_PASSWORD)."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "verify_certs",
            "label": "Verify TLS certificates",
            "description": "Enable TLS certificate verification (default: true).",
            "type": "checkbox",
            "required": False,
            "default_value": True,
        },
    ],
}


def _to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _build_es_client(task_config: dict[str, Any]) -> Elasticsearch:
    es_url = (
        task_config.get("elasticsearch_url")
        or os.getenv("ELASTICSEARCH_URL")
        or "http://elasticsearch:9200"
    )
    api_key = task_config.get("api_key") or os.getenv("ELASTICSEARCH_API_KEY")
    username = task_config.get("username") or os.getenv("ELASTICSEARCH_USERNAME")
    password = task_config.get("password") or os.getenv("ELASTICSEARCH_PASSWORD")
    verify_certs = _to_bool(
        task_config.get("verify_certs", os.getenv("ELASTICSEARCH_VERIFY_CERTS")),
        default=True,
    )
    auth_mode = (task_config.get("auth_mode") or "").strip().lower()

    client_kwargs: dict[str, Any] = {"verify_certs": verify_certs}

    if auth_mode and auth_mode not in {"none", "api_key", "basic"}:
        raise RuntimeError("task_config.auth_mode must be one of: none, api_key, basic")

    if auth_mode == "api_key":
        if not api_key:
            raise RuntimeError(
                "task_config.api_key (or ELASTICSEARCH_API_KEY) "
                "is required for auth_mode=api_key"
            )
        client_kwargs["api_key"] = api_key
        return Elasticsearch(es_url, **client_kwargs)

    if auth_mode == "basic":
        if not (username and password):
            raise RuntimeError(
                "task_config.username/task_config.password (or ELASTICSEARCH_USERNAME/ELASTICSEARCH_PASSWORD) "
                "are required for auth_mode=basic"
            )
        client_kwargs["basic_auth"] = (username, password)
        return Elasticsearch(es_url, **client_kwargs)

    # Auto-detect auth when auth_mode is omitted.
    if api_key:
        client_kwargs["api_key"] = api_key
        return Elasticsearch(es_url, **client_kwargs)

    if username and password:
        client_kwargs["basic_auth"] = (username, password)
        return Elasticsearch(es_url, **client_kwargs)

    return Elasticsearch(es_url, **client_kwargs)


def _normalize_document(raw: str, parse_json_lines: bool) -> dict[str, Any]:
    if not parse_json_lines:
        return {"message": raw.rstrip("\n")}

    parsed = json.loads(raw)
    if isinstance(parsed, dict):
        return parsed

    return {"value": parsed}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def export(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Export upstream worker result files to Elasticsearch."""
    del output_path
    input_files = get_input_files(pipe_result, input_files or [])
    task_config = task_config or {}

    index_name = task_config.get("index_name")
    if not index_name:
        raise RuntimeError("task_config.index_name is required")

    id_field = task_config.get("id_field")
    parse_json_lines = _to_bool(task_config.get("parse_json_lines"), default=True)

    client = _build_es_client(task_config)

    indexed_documents = 0
    skipped_lines = 0

    for input_file in input_files:
        file_path = input_file.get("path")
        display_name = input_file.get("display_name")

        with open(file_path, encoding="utf-8") as file_handle:
            for line_number, line in enumerate(file_handle, start=1):
                if not line.strip():
                    continue

                try:
                    document = _normalize_document(line, parse_json_lines)
                except json.JSONDecodeError:
                    skipped_lines += 1
                    continue

                document.setdefault("openrelik_workflow_id", workflow_id)
                document.setdefault("openrelik_source_file", display_name)
                document.setdefault("openrelik_source_path", file_path)
                document.setdefault("openrelik_line_number", line_number)

                doc_id = document.get(id_field) if id_field else None

                if doc_id:
                    client.index(index=index_name, document=document, id=str(doc_id))
                else:
                    client.index(index=index_name, document=document)
                indexed_documents += 1

                if indexed_documents % 100 == 0:
                    self.send_event(
                        "task-progress",
                        data={
                            "indexed_documents": indexed_documents,
                            "skipped_lines": skipped_lines,
                            "index_name": index_name,
                        },
                    )

    return create_task_result(
        output_files=[],
        workflow_id=workflow_id,
        command="elasticsearch.index",
        meta={
            "index_name": index_name,
            "indexed_documents": indexed_documents,
            "skipped_lines": skipped_lines,
        },
    )
