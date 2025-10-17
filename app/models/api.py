from pydantic import BaseModel, EmailStr, ConfigDict, Field, field_validator
from typing import List
from schema.schema import ClientRoleEnum
from app.constants.content_type import ALLOWED_EXTENSIONS
import os


class StandardResponse(BaseModel):
    message: str


class UserClientBase(BaseModel):
    email: EmailStr
    role: ClientRoleEnum


class UserClientCreated(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int


class RegisterUser(BaseModel):
    email: EmailStr


class IndividualListedUser(BaseModel):
    id: int
    email: str
    role: ClientRoleEnum

    model_config = ConfigDict(from_attributes=True)


class ListUsers(StandardResponse):
    users: List[IndividualListedUser]


class GeneratedToken(StandardResponse):
    token: str


class GeneratedApiKey(StandardResponse):
    api_key: str


class GeneratedUrls(BaseModel):
    id: int
    url: str


class GeneratedPresignedUrls(StandardResponse):
    urls: List[GeneratedUrls]


class GeneratePresignedUrlsReq(BaseModel):
    files: List[str] = Field(
        ..., min_length=1, description="a list of filenames to upload"
    )

    @field_validator("files", mode="before")
    @classmethod
    def check_file_extension(cls, files: List[str]) -> str:
        for filename in files:
            _root, extension = os.path.splitext(filename)

            if not extension:
                raise ValueError(f"File '{filename}' has not extension")

            if extension.lower() not in ALLOWED_EXTENSIONS:
                raise ValueError(
                    f"file type for '{filename}' is not allowed",
                    f"allowed extension are: {','.join(ALLOWED_EXTENSIONS)}",
                )

        return files

    class Config:
        json_schema_extra = {"example": {"files": ["mydocument.pdf", "data.csv"]}}


class FinalizeDocumentReq(BaseModel):
    failed: List[int]
    successful: List[int]

    class Config:
        json_schema_extra = {"example": {"failed": [1, 2, 3], "successful": [1, 2, 5]}}


class Document(BaseModel):
    id: int
    file_name: str

    class Config:
        from_attributes = True


class ListDocuments(StandardResponse):
    documents: List[Document]
    total_count: int

    class Config:
        from_attributes = True

