from fastapi import Request
from fastapi.responses import JSONResponse


class PipelineException(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(message)


async def pipeline_exception_handler(_: Request, exc: PipelineException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})
