
from fastapi import APIRouter
from app.models.api import StandardResponse

router = APIRouter(prefix="/health", tags=["Health"])

@router.get("/", response_model=StandardResponse)
def server_health_check() -> StandardResponse:
    return StandardResponse(message="server is healthy, up and running")

