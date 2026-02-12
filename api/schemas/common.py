from pydantic import BaseModel


class APIResponse(BaseModel):
    status: str = "ok"
    detail: str
    result: dict | None = None
