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
        },
    ],
}


def _build_es_client() -> Elasticsearch:
    es_url = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    password = os.getenv("ELASTICSEARCH_PASSWORD")

    if api_key:
        return Elasticsearch(es_url, api_key=api_key)

    if username and password:
        return Elasticsearch(es_url, basic_auth=(username, password))

    return Elasticsearch(es_url)


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
    parse_json_lines = task_config.get("parse_json_lines", True)

    client = _build_es_client()

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
