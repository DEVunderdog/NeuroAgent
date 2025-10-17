from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy import select
from app.models.database import VerifiedApiKey, StoreApiKey
from schema.schema import User, ApiKey
from typing import Tuple


async def get_api_key_for_verification(
    *, db: AsyncSession, api_key: bytes
) -> VerifiedApiKey:
    u = aliased(User)

    stmt = (
        select(ApiKey, u.role.label("role"))
        .join(u, ApiKey.user)
        .where(ApiKey.key_credential == api_key)
    )

    result = await db.execute(stmt)

    row = result.first()

    if not row:
        return None

    api_key_obj, role = row

    return VerifiedApiKey(
        id=api_key_obj.id,
        user_id=api_key_obj.user_id,
        user_role=role,
        key_id=api_key_obj.key_id,
        key_credential=api_key_obj.key_credential,
        key_signature=api_key_obj.key_signature,
    )


async def store_api_key(*, db: AsyncSession, api_key_params: StoreApiKey) -> Tuple[ApiKey, str]:
    try:
        api_key = ApiKey(
            user_id=api_key_params.user_id,
            key_id=api_key_params.key_id,
            key_credential=api_key_params.key_credential,
            key_signature=api_key_params.key_signature,
        )
        db.add(api_key)
        stmt = select(User.email).where(User.id == api_key_params.user_id)
        result = await db.execute(stmt)

        row = result.first()

        await db.commit()
        await db.refresh(api_key)
        return api_key, row.email
    except Exception as e:
        await db.rollback()
        raise RuntimeError(f"failed to store api key: {e}")
