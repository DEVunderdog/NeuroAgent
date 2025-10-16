from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
from app.utils.config import settings
from app.token.token_manager import TokenManager
from app.api.main import api_router
from app.exception.custom import request_validation_exception_handler
from app.aws.client import AwsClientManager
import logging
import sys

log_level = None

if settings.is_development:
    log_level = "DEBUG"
else:
    log_level = "INFO"

numeric_log_level = getattr(logging, log_level, logging.INFO)

logging.basicConfig(
    level=numeric_log_level,
    stream=sys.stdout,
    format="%(levelname)-8s [%(asctime)s] [%(name)s] %(message)s (%(filename)s:%(lineno)d)",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.aws_client_manager = AwsClientManager(settings=settings)

    logger.info("application server startup: initializing resources...")

    app.state.token_manager = await TokenManager.create(settings=settings)

    yield

    logger.info("application server is shutting down")

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)

app.add_exception_handler(RequestValidationError, request_validation_exception_handler)

app.include_router(api_router)
