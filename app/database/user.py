from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from pydantic import EmailStr
from schema.schema import User, ApiKey, ClientRoleEnum
from app.models.database import UserClientCreate, ApiKeyCreate
from typing import Tuple, List
import psycopg


class UserAlreadyExistsError(Exception):
    def __init__(self, email: str):
        self.email = email
        super().__init__(f"user with email '{email}' already exists")


async def register_user(
    *, db: AsyncSession, user_params: UserClientCreate, api_key_params: ApiKeyCreate
) -> Tuple[User, ApiKey]:
    try:
        user = User(**user_params.model_dump())
        db.add(user)

        api_key = ApiKey(
            user=user,
            key_id=api_key_params.key_id,
            key_credential=api_key_params.key_credential,
            key_signature=api_key_params.key_signature,
        )
        db.add(api_key)
        await db.commit()
        await db.refresh(user)
        await db.refresh(api_key)
        return user, api_key
    except IntegrityError as e:
        await db.rollback()
        if isinstance(e.orig, psycopg.errors.UniqueViolation):
            raise UserAlreadyExistsError(email=user.email) from e
    except Exception as e:
        await db.rollback()
        raise RuntimeError(f"failed to register user due to an unexpected error: {e}")


async def get_user_db(*, db: AsyncSession, email: EmailStr) -> User | None:
    stmt = select(User).where(User.email == email)
    result = await db.execute(stmt)
    return result.scalars().first()


async def list_users_db(
    *, db: AsyncSession, limit: int = 10, offset: int = 0
) -> List[User]:
    stmt = select(User).order_by(User.id).limit(limit=limit).offset(offset=offset)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def promote_user_db(*, db: AsyncSession, user_id: int) -> User | None:
    stmt = select(User).where(User.id == user_id)
    result = await db.execute(stmt)
    user = result.scalars().first()

    if user:
        if user.role == ClientRoleEnum.ADMIN:
            return user
        else:
            user.role = ClientRoleEnum.ADMIN
            try:
                await db.commit()
                return user
            except Exception:
                await db.rollback()
                raise
    else:
        return None


async def delete_user_db(*, db: AsyncSession, user_id: int) -> bool:
    stmt = select(User).where(User.id == user_id)
    result = await db.execute(stmt)
    user_client = result.scalars().first()

    if user_client:
        await db.delete(user_client)
        try:
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            raise
    else:
        return False
