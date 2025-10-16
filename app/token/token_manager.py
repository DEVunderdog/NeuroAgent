import logging
import secrets
from app.utils.config import Settings
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Tuple, Dict, Optional
from app.token.key_info import KeyInfo
from app.models.token import TokenData
from app.database.encryption_key import get_active_encryption_key
from app.database.connection import SessionLocal
from datetime import timedelta
from app.utils.application_timezone import get_current_time
from jose import JWTError, jwt
from jose.constants import ALGORITHMS
import base64
import os
import hashlib
import hmac

logger = logging.getLogger(__name__)


class KeyNotFoundError(Exception):
    pass


class SigningKeyExpired(Exception):
    pass


class NotFoundApiKey(Exception):
    pass


class SigningKeyNotFoundError(Exception):
    pass


class TokenManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._active_key_config: Optional[Tuple[Dict[int, KeyInfo], int]] = None

    @classmethod
    async def create(
        cls,
        settings: Settings,
    ) -> "TokenManager":
        instance = cls(settings)

        async with SessionLocal() as db:
            instance._active_key_config = await instance._build_active_key(db=db)

        if instance._active_key_config is None:
            raise RuntimeError("failed to initialize TokenManager with encryption keys")

        return instance

    async def _build_active_key(
        self, db: AsyncSession
    ) -> Tuple[Dict[int, KeyInfo], int]:
        active_encryption_key = await get_active_encryption_key(db=db)
        active_id: Optional[int] = None
        key_info: Dict[int, KeyInfo] = {}
        decrypted_key_info: Dict[int, KeyInfo] = {}

        if active_encryption_key is None:
            raise SigningKeyNotFoundError("cannot find the active signing keys")

        active_id = active_encryption_key.id
        key_info[active_id] = KeyInfo(
            key=active_encryption_key.symmetric_key,
            expires_at=active_encryption_key.expired_at,
        )

        for key_id_iter, value in key_info.items():
            decrypted_key_info[key_id_iter] = KeyInfo(
                key=value.key, expires_at=value.expires_at
            )

        return (decrypted_key_info, active_id)

    def get_keys(self) -> Tuple[Dict[int, KeyInfo], int]:
        return self._active_key_config

    def create_access_token(
        self, payload_data: TokenData, expires_delta: Optional[timedelta] = None
    ) -> str:
        all_keys, active_key_id = self.get_keys()
        if active_key_id not in all_keys:
            raise KeyNotFoundError(
                "active key id not found in current key configuration"
            )

        active_key_info = all_keys[active_key_id]
        if active_key_info.is_expired():
            raise SigningKeyExpired("signing key is expired")

        to_encode = payload_data.model_dump(mode="json", exclude_unset=True)

        expire = None
        current_time = get_current_time()
        if expires_delta:
            expire = current_time + expires_delta
        else:
            expire = current_time + timedelta(
                hours=self.settings.JWT_ACCESS_TOKEN_HOURS
            )

        to_encode.update(
            {
                "exp": expire,
                "iss": self.settings.JWT_ISSUER,
                "aud": self.settings.JWT_AUDIENCE,
                "iat": get_current_time(),
                "jti": os.urandom(16).hex(),
            }
        )

        headers = {"kid": active_key_id}
        encoded_jwt = jwt.encode(
            to_encode,
            active_key_info.key.hex(),
            algorithm=ALGORITHMS.HS256,
            headers=headers,
        )
        return encoded_jwt

    def verify_token(self, token: str) -> Optional[TokenData]:
        try:
            unverified_headers = jwt.get_unverified_header(token=token)
            kid = unverified_headers.get("kid")
            if not kid:
                raise RuntimeError("token missing 'kid' header")

            all_keys, _ = self.get_keys()

            key_for_verification = all_keys.get(kid)
            if not key_for_verification:
                raise KeyNotFoundError(f"key id {kid} not found for verification")

            if key_for_verification.is_expired():
                raise RuntimeError(
                    f"symmetric key {kid} for token verification has expired"
                )

            payload = jwt.decode(
                token=token,
                key=key_for_verification.key.hex(),
                algorithms=[ALGORITHMS.HS256],
                audience=self.settings.JWT_AUDIENCE,
                issuer=self.settings.JWT_ISSUER,
                options={"verify_aud": True, "verify_iss": True, "verify_exp": True},
            )
            return TokenData(**payload)
        except jwt.ExpiredSignatureError:
            logger.error("token has expired")
            raise
        except jwt.JWTClaimsError as e:
            logger.error(f"token claims error: {e}")
            raise
        except JWTError as e:
            logger.error(f"invalid token: {e}")
            raise

    def generate_api_key(self) -> Tuple[str, bytes, bytes, int]:
        all_keys, active_key_id = self.get_keys()

        random_bytes = secrets.token_bytes(24)
        random_bytes_b64 = (
            base64.urlsafe_b64encode(random_bytes).decode("utf-8").rstrip("=")
        )

        active_key_info = all_keys.get(active_key_id)
        if not active_key_info:
            raise KeyNotFoundError(f"key id {active_key_id} not found for verification")

        if active_key_info.is_expired():
            raise RuntimeError(f"key id {active_key_id} has been expired")

        data_to_hmac = f"{active_key_id}:{random_bytes_b64}".encode("utf-8")
        hmac_obj = hmac.new(active_key_info.key, data_to_hmac, hashlib.sha256)
        signature_bytes = hmac_obj.digest()
        signature_b64 = (
            base64.urlsafe_b64encode(signature_bytes).decode("utf-8").rstrip("=")
        )

        api_key = f"{random_bytes_b64}.{signature_b64}"

        api_key_bytes = api_key.encode("utf-8")

        return api_key, api_key_bytes, signature_bytes, active_key_id

    def verify_api_key(self, api_key: str, key_hmac: bytes, kid: int) -> bool:
        parts = api_key.split(".")
        if len(parts) != 2:
            return False

        random_bytes_b64, signature_b64 = parts
        all_keys, _ = self.get_keys()

        key_info = all_keys.get(kid)
        if not key_info:
            logger.info("cannot find key infor while verifying the api key")
            return False

        if key_info.is_expired():
            raise RuntimeError(
                f"symmetric key {kid} for token verification has expired"
            )

        data_to_hmac = f"{kid}:{random_bytes_b64}".encode("utf-8")

        expected_hmac_obj = hmac.new(key_info.key, data_to_hmac, hashlib.sha256)
        expected_signature_bytes = expected_hmac_obj.digest()

        try:
            client_signature_bytes = base64.urlsafe_b64decode(signature_b64 + "==")
        except Exception as e:
            logger.error(f"error while decoding signature and providing key hmac: {e}")
            return False

        return hmac.compare_digest(
            expected_signature_bytes, client_signature_bytes
        ) and hmac.compare_digest(expected_signature_bytes, key_hmac)
