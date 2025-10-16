from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime
from sqlalchemy import (
    TIMESTAMP,
    Integer,
    Identity,
    LargeBinary,
    Boolean,
    text,
    BigInteger,
    String,
    Enum as SqlEnum,
    ForeignKey,
)
from sqlalchemy.sql import func
from typing import Optional, List
import enum


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ClientRoleEnum(enum.Enum):
    USER = "USER"
    ADMIN = "ADMIN"


class OperationStatusEnum(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class ProvisionerStatusEnum(enum.Enum):
    PROVISIONING = "PROVISIONING"
    AVAILABLE = "AVAILABLE"
    ASSIGNED = "ASSIGNED"
    DESTROYED = "DESTROYED"
    CLEANUP = "CLEANUP"
    FAILED = "FAILED"


class EncryptionKey(Base, TimestampMixin):
    __tablename__ = "encryption_keys"

    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    symmetric_key: Mapped[bytes] = mapped_column(LargeBinary)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    expired_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    api_keys: Mapped[List["ApiKey"]] = relationship(back_populates="encryption_key")

    def __repr__(self):
        return f"<EncryptionKey(id={self.id}, is_active={self.is_active})>"


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    role: Mapped[ClientRoleEnum] = mapped_column(
        SqlEnum(ClientRoleEnum, name="client_roles", create_type=False), nullable=False
    )

    api_keys: Mapped[List["ApiKey"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    documents: Mapped[List["DocumentRegistry"]] = relationship(
        back_populates="user"
    )

class ApiKey(Base, TimestampMixin):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    key_id: Mapped[int] = mapped_column(
        ForeignKey("encryption_keys.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    key_credential: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False, unique=True
    )
    key_signature: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    user: Mapped["User"] = relationship(back_populates="api_keys")
    encryption_key: Mapped["EncryptionKey"] = relationship(back_populates="api_keys")

    def __repr__(self):
        return f"<ApiKey(id={self.id}, user_id={self.user_id}, key_id={self.key_id})>"


class DocumentRegistry(Base, TimestampMixin):
    __tablename__ = "documents_registry"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    file_name: Mapped[str] = mapped_column(String(100), nullable=False)
    object_key: Mapped[str] = mapped_column(String(150), nullable=False)
    lock_status: Mapped[bool] = mapped_column(Boolean, nullable=False)
    op_status: Mapped[OperationStatusEnum] = mapped_column(
        SqlEnum(OperationStatusEnum, name="operation_status", create_type=False),
        server_default=OperationStatusEnum.PENDING.value,
    )

    user: Mapped["User"] = relationship(back_populates="documents")

    def __repr__(self):
        return f"<DocumentRegistry(id={self.id}, file_name={self.file_name}, user_id={self.user_id})>"
