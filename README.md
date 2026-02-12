# items_pipeline

## Run locally

```bash
pip install -r requirements.txt
uvicorn api.app:app --reload --host 0.0.0.0 --port 8080
```

## Endpoints

- GET `/health/`
- POST `/v1/index/create/`
- POST `/v1/embed_data/`
- POST `/v1/streaming/update/`
- POST `/v1/streaming/delete/`
- POST `/v1/endpoint/create/`
- POST `/v1/endpoint/deploy/`
- POST `/v1/search`

Default values for optional fields are stored in `functions/parameters/config.yaml`.
