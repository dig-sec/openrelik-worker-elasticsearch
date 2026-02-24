# openrelik-worker-elasticsearch

OpenRelik worker that exports line-based worker results (JSONL or raw lines) to an Elasticsearch index.

## What this worker does

- Accepts files from upstream workflow workers.
- Reads each line from each file.
- Indexes JSON documents (or raw lines as text) into Elasticsearch.
- Adds OpenRelik context fields (`openrelik_workflow_id`, source file/path, line number).

## Configuration

### Environment variables

- `REDIS_URL` (required for Celery)
- `ELASTICSEARCH_URL` (default: `http://elasticsearch:9200`)
- `ELASTICSEARCH_API_KEY` (optional)
- `ELASTICSEARCH_USERNAME` and `ELASTICSEARCH_PASSWORD` (optional basic auth)

### Task config

- `index_name` (required): target Elasticsearch index.
- `id_field` (optional): JSON field to use as `_id`.
- `parse_json_lines` (optional, default `true`): parse each line as JSON. If disabled, line is indexed as `{ "message": "..." }`.

## Local testing

```bash
python -m pip install -e . pytest
pytest
```
