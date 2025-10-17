from fastapi import APIRouter, status, HTTPException
from app.database.user import register_user, UserAlreadyExistsError
from app.api.deps import TokenDep, SessionDep, TokenPayloadDep
from app.models.api import RegisterUser, StandardResponse, ListUsers
from app.models.database import UserClientCreate, ApiKeyCreate
from schema.schema import ClientRoleEnum
from app.token.token_manager import KeyNotFoundError
from app.mail.send_mail import send_api_mail
from app.utils.config import settings
from app.database.user import list_users_db, promote_user_db, delete_user_db
from app.constants.globals import unauthorized_msg

import logging

router = APIRouter(prefix="/user", tags=["User"])

logger = logging.getLogger(__name__)


@router.post(
    "/register",
    response_model=StandardResponse,
    status_code=status.HTTP_201_CREATED,
    summary="register a new user and provision an api key via mail by Admin",
)
async def register_user_to_server(
    user_req: RegisterUser,
    db: SessionDep,
    token_manager: TokenDep,
):
    try:

        api_key, api_key_bytes, api_key_signature, active_key_id = (
            token_manager.generate_api_key()
        )
        user = UserClientCreate(email=user_req.email, role=ClientRoleEnum.USER)
        api_key_params = ApiKeyCreate(
            key_id=active_key_id,
            key_credential=api_key_bytes,
            key_signature=api_key_signature,
        )

        await register_user(db=db, user_params=user, api_key_params=api_key_params)

        send_api_mail(email_to=user_req.email, api_key=api_key, settings=settings)
    except UserAlreadyExistsError:
        msg = "user already exists"
        logger.error(msg, exc_info=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    except KeyNotFoundError:
        logger.error("cannot create api key while registering user", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detaiil="error while creating api keys",
        )
    except RuntimeError:
        msg = "cannot create api key while registering user"
        logger.error(msg, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )
    except HTTPException:
        raise
    except Exception:
        msg = "error registering user and storing it"
        logger.error(msg, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )
    
    return StandardResponse(
        message="successfully created register, please check your mail for API keys."
    )


@router.get(
    "/list",
    response_model=ListUsers,
    status_code=status.HTTP_200_OK,
    summary="list of users",
)
async def list_users(
    admin_payload: TokenPayloadDep, db: SessionDep, limit: int = 10, offset: int = 0
):
    if admin_payload.role != ClientRoleEnum.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=unauthorized_msg,
        )

    users = await list_users_db(db=db, limit=limit, offset=offset)
    return ListUsers(message="successfully fetched users", users=users)


@router.patch(
    "/promote/{user_id}",
    response_model=StandardResponse,
    status_code=status.HTTP_200_OK,
    summary="promote users to admin",
)
async def promote_users(user_id: int, db: SessionDep, admin_payload: TokenPayloadDep):
    if admin_payload.role != ClientRoleEnum.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=unauthorized_msg,
        )

    if user_id == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide valid user_id to promote",
        )

    try:
        user_client = await promote_user_db(db=db, user_id=user_id)
        if user_client is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cannot find user with provided id",
            )
    except HTTPException:
        raise
    except Exception:
        msg = "error promoting user to admin"
        logger.error(msg, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )

    return StandardResponse(message="successfully promoted user to admin")


@router.delete(
    "/delete/{user_id}",
    response_model=StandardResponse,
    status_code=status.HTTP_200_OK,
    summary="delete users",
)
async def delete_user(user_id: int, db: SessionDep, admin_payload: TokenPayloadDep):
    if admin_payload.role != ClientRoleEnum.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=unauthorized_msg
        )

    if user_id == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide user_id to delete",
        )

    if admin_payload.user_id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="you cannot delete yourself"
        )

    try:
        deleted = await delete_user_db(db=db, user_id=user_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cannot find user with provided id",
            )
    except HTTPException:
        raise
    except Exception:
        msg = "error deleting user"
        logger.error(msg, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )
    
    return StandardResponse(message="user deleted successfully")
