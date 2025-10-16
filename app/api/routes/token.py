from fastapi import APIRouter, status, HTTPException
from app.models.api import GeneratedToken, StandardResponse
from app.models.database import StoreApiKey
from app.models.token import TokenData
from app.token.token_manager import KeyNotFoundError
from app.api.deps import ApiPayloadDep, SessionDep, TokenDep
from app.database.api_key import store_api_key
from app.mail.send_mail import send_api_mail
from app.utils.config import settings
import logging


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth/generate", tags=["Token"])


@router.get(
    "/token",
    response_model=GeneratedToken,
    status_code=status.HTTP_201_CREATED,
    summary="generated token based on API Key",
)
def generate_token(token_manager: TokenDep, payload: ApiPayloadDep):
    try:
        data = TokenData(user_id=payload.user_id, role=payload.role)
        token = token_manager.create_access_token(payload_data=data)
    except Exception as e:
        msg = f"error generating token: {e}"
        logger.error(msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )
    return GeneratedToken(message="generated token successfully", token=token)


@router.get(
    "/key",
    response_model=StandardResponse,
    status_code=status.HTTP_201_CREATED,
    summary="generate api keys for user",
)
async def generate_user_api_keys(
    db: SessionDep, token_manager: TokenDep, payload: ApiPayloadDep
):
    try:
        new_api_key, new_api_key_bytes, new_api_key_signature, active_key_id = (
            token_manager.generate_api_key()
        )
        db_api_key, email = await store_api_key(
            db=db,
            api_key_params=StoreApiKey(
                user_id=payload.user_id,
                key_id=active_key_id,
                key_credential=new_api_key_bytes,
                key_signature=new_api_key_signature,
            ),
        )
        send_api_mail(email_to=email, api_key=new_api_key, settings=settings)
    except KeyNotFoundError:
        logger.error("cannot create api key, because signing key not found")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="error while creating api key",
        )
    except HTTPException:
        raise
    except Exception as e:
        msg = f"error generating api key: {e}"
        logger.error(msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )

    return StandardResponse(
        message="successfully generated api key, please check your mail"
    )
