from pydantic import BaseModel
from schema.schema import ClientRoleEnum
from app.models.api import UserClientBase


class VerifiedApiKey(BaseModel):
    id: int
    user_id: int
    user_role: ClientRoleEnum
    key_id: int
    key_credential: bytes
    key_signature: bytes


class UserClientCreate(UserClientBase):
    pass


class ApiKeyCreate(BaseModel):
    key_id: int
    key_credential: bytes
    key_signature: bytes


class StoreApiKey(ApiKeyCreate):
    user_id: int


class CreateDocument(BaseModel):
    user_id: int
    file_name: str
    object_key: str
