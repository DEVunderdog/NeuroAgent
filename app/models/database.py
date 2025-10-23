from pydantic import BaseModel
from typing import List
from schema.schema import ClientRoleEnum
from app.models.api import UserClientBase
from app.models.aws import FileForIngestion


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


class CreateKbParams(BaseModel):
    user_id: int
    name: str


class KbDoc(BaseModel):
    doc_id: int
    kb_doc_id: int
    file_name: str
    status: str


class ListedKbDocs(BaseModel):
    kb_docs: List[KbDoc]
    total_count: int
    knowledge_base_id: int


class CreatedIngestionJob(BaseModel):
    ingestion_id: int
    index_arn: str
    kb_id: int
    user_id: int
    documents: List[FileForIngestion]
