from unittest import mock

import pytest

from src import tasks


def test_normalize_document_json_dict():
    result = tasks._normalize_document('{"event":"login"}\n', True)
    assert result == {"event": "login"}


def test_normalize_document_json_scalar():
    result = tasks._normalize_document('"hello"\n', True)
    assert result == {"value": "hello"}


def test_normalize_document_raw_text():
    result = tasks._normalize_document("line\n", False)
    assert result == {"message": "line"}


@mock.patch("src.tasks.Elasticsearch")
def test_build_es_client_with_api_key(mock_es):
    with mock.patch.dict(
        "os.environ",
        {
            "ELASTICSEARCH_URL": "http://localhost:9200",
            "ELASTICSEARCH_API_KEY": "token",
        },
        clear=True,
    ):
        tasks._build_es_client()

    mock_es.assert_called_once_with("http://localhost:9200", api_key="token")


@mock.patch("src.tasks.create_task_result")
@mock.patch("src.tasks.get_input_files")
@mock.patch("src.tasks._build_es_client")
def test_export_success(mock_build_client, mock_get_input_files, mock_create_task_result, tmp_path):
    input_file = tmp_path / "results.jsonl"
    input_file.write_text('{"id":"1","message":"ok"}\n{"id":"2","message":"ok2"}\n')

    mock_get_input_files.return_value = [
        {"path": str(input_file), "display_name": "results.jsonl"}
    ]

    mock_client = mock.Mock()
    mock_build_client.return_value = mock_client
    mock_create_task_result.return_value = "encoded-result"

    result = tasks.export.run(
        pipe_result="encoded",
        workflow_id="wf-1",
        task_config={"index_name": "openrelik-results", "id_field": "id"},
    )

    assert result == "encoded-result"
    assert mock_client.index.call_count == 2

    first_call = mock_client.index.call_args_list[0]
    assert first_call.kwargs["index"] == "openrelik-results"
    assert first_call.kwargs["id"] == "1"
    assert first_call.kwargs["document"]["openrelik_workflow_id"] == "wf-1"


@mock.patch("src.tasks.get_input_files")
def test_export_requires_index_name(mock_get_input_files):
    mock_get_input_files.return_value = []

    with pytest.raises(RuntimeError, match="index_name"):
        tasks.export.run(task_config={})
