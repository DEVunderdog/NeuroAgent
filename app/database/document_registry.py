from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import insert, update, case, cast, select, delete, not_, and_
from sqlalchemy.sql import func
from typing import List, Tuple
from app.models.database import CreateDocument
from schema.schema import OperationStatusEnum, DocumentRegistry, KnowledgeBaseDocument
import logging

logger = logging.getLogger(__name__)


class DocumentInKnowledgeBaseError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


async def create_document(
    *, db: AsyncSession, docs: List[CreateDocument]
) -> List[Tuple[int, str]]:
    try:
        documents_data = [
            {
                "user_id": doc.user_id,
                "file_name": doc.file_name,
                "object_key": doc.object_key,
                "lock_status": True,
                "op_status": OperationStatusEnum.PENDING,
            }
            for doc in docs
        ]

        stmt = insert(DocumentRegistry).returning(
            DocumentRegistry.id, DocumentRegistry.file_name
        )

        result = await db.execute(stmt, documents_data)

        created_documents = [(row.id, row.file_name) for row in result.fetchall()]

        await db.commit()

        logger.info(f"successfully created {len(created_documents)} documents")

        return created_documents

    except IntegrityError:
        await db.rollback()
        logging.error("integrity error during creating documents")
        raise ValueError("duplicate file names or constraint violation")

    except Exception as e:
        await db.rollback()
        logging.error(f"error during bulk document creation: {e}")
        raise


async def finalize_documents(
    *, db: AsyncSession, successful: List[int], failed: List[int]
):
    try:
        all_ids = successful + failed

        stmt = (
            update(DocumentRegistry)
            .where(DocumentRegistry.id.in_(all_ids))
            .values(
                op_status=case(
                    (
                        DocumentRegistry.id.in_(successful),
                        cast(
                            OperationStatusEnum.SUCCESS.value,
                            DocumentRegistry.op_status.type,
                        ),
                    ),
                    (
                        DocumentRegistry.id.in_(failed),
                        cast(
                            OperationStatusEnum.FAILED.value,
                            DocumentRegistry.op_status.type,
                        ),
                    ),
                ),
                lock_status=False,
            )
        )

        await db.execute(stmt)
        await db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"error finalizing documents in database: {e}")
        raise


async def list_files(
    *, db: AsyncSession, user_id: int, limit: int, offset: int
) -> Tuple[List[DocumentRegistry], int]:
    try:
        stmt = select(DocumentRegistry).where(
            DocumentRegistry.user_id == user_id,
            DocumentRegistry.lock_status == False,
            DocumentRegistry.op_status == OperationStatusEnum.SUCCESS,
        )

        count_stmt = select(func.count()).select_from(stmt.subquery())

        count_result = await db.execute(count_stmt)

        total_count = count_result.scalar()

        stmt = stmt.limit(limit=limit)
        stmt = stmt.offset(offset=offset)

        result = await db.execute(stmt)

        documents = result.scalars().all()

        return documents, total_count
    except Exception as e:
        logger.error(f"error listing users documents from database: {e}")
        raise


async def delete_documents(*, db: AsyncSession, document_ids: List[int], user_id: int):
    try:
        stmt = delete(DocumentRegistry).where(
            DocumentRegistry.id.in_(document_ids),
            DocumentRegistry.op_status == OperationStatusEnum.PENDING,
            DocumentRegistry.lock_status is True,
            DocumentRegistry.user_id == user_id,
        )

        await db.execute(stmt)
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.error(f"error during deleting file in database: {e}")
        raise


async def conflicted_docs(*, db: AsyncSession) -> List[DocumentRegistry]:
    try:
        stmt = select(DocumentRegistry).where(
            not_(
                and_(
                    DocumentRegistry.lock_status == False,
                    DocumentRegistry.op_status == OperationStatusEnum.SUCCESS,
                )
            )
        )

        result = await db.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"error while fetching conflicted documents: {e}")
        raise


async def cleanup_docs(
    *, db: AsyncSession, to_be_unlocked: List[int], to_be_deleted: List[int]
):
    try:
        if to_be_deleted:
            stmt = delete(DocumentRegistry).where(
                DocumentRegistry.id.in_(to_be_deleted)
            )
            await db.execute(stmt)

        if to_be_unlocked:
            stmt = (
                update(DocumentRegistry)
                .where(DocumentRegistry.id.in_(to_be_unlocked))
                .values(lock_status=False, op_status=OperationStatusEnum.SUCCESS)
            )
            await db.execute(stmt)
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.error(f"error cleaning up docs in database: {e}")
        raise


async def lock_documents(
    *,
    db: AsyncSession,
    document_ids: List[int],
    user_id: int,
    for_deletion: bool = True,
) -> List[str]:
    try:
        query = select(KnowledgeBaseDocument.document_id).where(
            KnowledgeBaseDocument.document_id.in_(document_ids)
        )
        result = await db.execute(query)
        existing_docs = result.scalars().all()

        if for_deletion and existing_docs:
            raise DocumentInKnowledgeBaseError(
                f"document with IDs {existing_docs} are in knowledge base"
            )

        stmt = (
            update(DocumentRegistry)
            .where(
                not DocumentRegistry.lock_status,
                DocumentRegistry.id.in_(document_ids),
                DocumentRegistry.op_status == OperationStatusEnum.SUCCESS,
                DocumentRegistry.user_id == user_id,
            )
            .values(lock_status=True, op_status=OperationStatusEnum.PENDING)
            .returning(DocumentRegistry.object_key)
        )
        result = await db.execute(stmt)
        object_keys = [row.object_key for row in result.fetchall()]
        await db.commit()
        return object_keys
    except Exception as e:
        await db.rollback()
        logger.error(f"error locking the documents: {str(e)}")
        raise
