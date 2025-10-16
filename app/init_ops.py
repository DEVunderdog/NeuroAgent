import logging
import asyncio
from tenacity import after_log, before_log, retry, stop_after_attempt, wait_fixed
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from app.utils.config import settings
from sqlalchemy import text
from app.database.encryption_key import get_active_encryption_key, create_encryption_key
from app.token.symmetric_key import generate_symmetric_key
from app.database.connection import SessionLocal, engine
from app.database.user import get_user_db, register_user
from app.token.token_manager import TokenManager, KeyNotFoundError
from app.models.database import UserClientCreate, ApiKeyCreate
from app.database.schema import ClientRoleEnum
from app.mail.send_mail import send_api_mail


logger = logging.getLogger(__name__)

max_tries = 60 * 5
wait_seconds = 3


@retry(
    stop=stop_after_attempt(max_tries),
    wait=wait_fixed(wait_seconds),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.WARNING),
    reraise=True,
)
async def check_db_ready() -> None:
    try:
        async_engine = create_async_engine(
            str(settings.DATABASE_URI), pool_pre_ping=True
        )
        async with async_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        logger.info("database connection successful")
    except Exception as e:
        logger.error(f"database connection failed: {e}")
        raise


async def check_for_active_key(db: AsyncSession) -> None:
    try:
        active_encryption_key = await get_active_encryption_key(db=db)
        if active_encryption_key is None:
            symmetric_key = generate_symmetric_key()
            await create_encryption_key(db=db, symmetric_key=symmetric_key)
    except Exception as e:
        logger.error(f"error checking for active signing keys: {e}")
        raise


async def create_admin_user(db: AsyncSession) -> None:
    logger.info(f"checking for existing admin user: {settings.FIRST_ADMIN}")
    existing_admin = await get_user_db(db=db, email=settings.FIRST_ADMIN)

    if existing_admin:
        logger.info("admin user already exists, no actions needed.")
        return

    logger.info("admin user not found, proceeding with creation")
    try:
        token_manager = await TokenManager.create(settings=settings)
        api_key, api_key_bytes, signature, _ = token_manager.generate_api_key()

        user_create = UserClientCreate(
            email=settings.FIRST_ADMIN, role=ClientRoleEnum.ADMIN
        )

        _, active_key_id = token_manager.get_keys()

        api_key_create = ApiKeyCreate(
            key_id=active_key_id, key_credential=api_key_bytes, key_signature=signature
        )

        await register_user(
            db=db, user_params=user_create, api_key_params=api_key_create
        )

        send_api_mail(email_to=settings.FIRST_ADMIN, api_key=api_key, settings=settings)
        logger.info("successfully created and notified the admin")
    except (KeyNotFoundError, RuntimeError, Exception) as e:
        logger.exception(f"exception occurred while creating admin: {e}")
        raise


async def main() -> None:
    logger.info("starting initial operations")

    await check_db_ready()

    async with SessionLocal() as session:
        await check_for_active_key(db=session)
        await create_admin_user(db=session)

    logger.info("initial operation completed")

    await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.error("initial application setup failed")
        raise
