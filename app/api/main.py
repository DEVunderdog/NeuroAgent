from fastapi import APIRouter
from app.api.routes import health
from app.api.routes import user
from app.api.routes import token
from app.api.routes import document_registry

api_router = APIRouter(prefix="/v1")
api_router.include_router(health.router)
api_router.include_router(user.router)
api_router.include_router(token.router)
api_router.include_router(document_registry.router)
