import asyncio
import logging
from app.utils.config import settings
from app.provisioner.manager import ProvisionManager
from app.aws.client import AwsClientManager


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def prime_index_pool() -> None:
    logger.info("priming index pool....")
    try:
        aws_client_manager = AwsClientManager(settings=settings)
        provision_manager = ProvisionManager(aws_client=aws_client_manager)

        await provision_manager.reconcile_vector_indexes()
        logger.info("successfully primed index pool")
    except Exception as e:
        logger.error(f"failed to prime index pool: {e}")
        raise


async def main() -> None:
    logger.info("initialing pre-startup service...")
    await prime_index_pool()
    logger.info("service finish initializing")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(
            f"a critical error occurred during pre-start of application: {e}"
        )
        exit(1)
