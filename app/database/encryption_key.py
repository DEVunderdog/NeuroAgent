from sqlalchemy.ext.asyncio import AsyncSession
from app.database.schema import EncryptionKey
from sqlalchemy import select


async def get_active_encryption_key(*, db: AsyncSession) -> EncryptionKey | None:
    stmt = select(EncryptionKey).where(EncryptionKey.is_active.is_(True))
    result = await db.execute(stmt)
    key = result.scalars().first()
    return key


async def create_encryption_key(*, db: AsyncSession, symmetric_key: bytes) -> int:
    new_key = EncryptionKey(symmetric_key=symmetric_key, is_active=True)
    db.add(new_key)
    try:
        await db.commit()
        await db.refresh(new_key)
        return new_key.id
    except Exception as e:
        await db.rollback()
        raise RuntimeError(f"failed to create encryption key: {e}") from e
