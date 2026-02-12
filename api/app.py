import time

from fastapi import FastAPI, Request

from api.exceptions import PipelineException, pipeline_exception_handler
from api.routes.health import router as health_router
from api.routes.index import router as index_router
from api.routes.embedding import router as embedding_router
from api.routes.streaming import router as streaming_router
from api.routes.endpoint import router as endpoint_router
from api.routes.search import router as search_router


def create_app() -> FastAPI:
    app = FastAPI(title="Items Pipeline API", version="1.0.0")
    app.add_exception_handler(PipelineException, pipeline_exception_handler)

    @app.middleware("http")
    async def add_response_time_header(request: Request, call_next):
        start = time.monotonic()
        request.state.start_time = start
        response = await call_next(request)
        elapsed = time.monotonic() - start
        response.headers["x-response-time-seconds"] = f"{elapsed:.6f}"
        return response

    app.include_router(health_router)
    app.include_router(index_router)
    app.include_router(embedding_router)
    app.include_router(streaming_router)
    app.include_router(endpoint_router)
    app.include_router(search_router)

    return app


app = create_app()
