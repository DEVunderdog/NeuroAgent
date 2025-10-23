from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
from app.utils.config import settings
from app.token.token_manager import TokenManager
from app.api.main import api_router
from app.exception.custom import request_validation_exception_handler
from app.aws.client import AwsClientManager
from app.provisioner.manager import ProvisionManager
from app.utils.scheduler import scheduler
import logging
import sys
import asyncio

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


def create_robust_task(coro, task_name: str):
    async def task_wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            logger.info(f"Task '{task_name}' was cancelled")
        except Exception:
            logger.critical(
                f"critical unhandled exception in background task '{task_name}'"
            )

    return asyncio.create_task(task_wrapper(), name=task_name)


async def schedule_cleanup_job(provision_manager: ProvisionManager):
    logger.info("schedule starting 'cleanup indexes' job")
    try:
        await provision_manager.cleanup_indexes()
    except Exception as e:
        logger.error(f"scheduled 'cleanup indexes' job failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.aws_client_manager = AwsClientManager(settings=settings)

    logger.info("application server startup: initializing resources...")

    app.state.token_manager = await TokenManager.create(settings=settings)

    provision_manager = ProvisionManager(aws_client=app.state.aws_client_manager)

    app.state.provision_manager = provision_manager

    reconciliation_task = create_robust_task(
        provision_manager.reconcilation_worker(), "reconciliation_worker"
    )

    cleanup_task = create_robust_task(
        provision_manager.cleanup_worker(), "cleanup_worker"
    )

    scheduler.add_job(
        schedule_cleanup_job,
        "cron",
        hour=8,
        minute=3,
        name="daily_cleanup",
        args=[provision_manager],
    )
    scheduler.start()

    yield

    logger.info("application server is shutting down")

    if scheduler.running:
        scheduler.shutdown()

    reconciliation_task.cancel()
    cleanup_task.cancel()

    try:
        await reconciliation_task
        await cleanup_task
    except asyncio.CancelledError:
        logger.info(
            "reconciliation worker task and cleanup task has been cancelled and stopped"
        )


app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)

app.add_exception_handler(RequestValidationError, request_validation_exception_handler)

app.include_router(api_router)
