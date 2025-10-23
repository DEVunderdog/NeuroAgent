from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from sqlalchemy.sql import func
from sqlalchemy.exc import NoResultFound, IntegrityError
from typing import Tuple, List
from schema.schema import (
    VectorIndex,
    ProvisionerStatusEnum,
    KnowledgeBase,
    DocumentRegistry,
    KnowledgeBaseDocument,
    OperationStatusEnum,
)
from sqlalchemy.orm import selectinload
from app.models.database import CreateKbParams, ListedKbDocs, KbDoc


async def create_kb_db(*, db: AsyncSession, arg: CreateKbParams) -> KnowledgeBase:
    try:
        async with db.begin():
            stmt = (
                select(VectorIndex)
                .where(
                    VectorIndex.status == ProvisionerStatusEnum.AVAILABLE,
                )
                .order_by(func.random())
                .limit(1)
                .with_for_update(skip_locked=True)
            )

            result = await db.execute(stmt)

            available_index = result.scalar_one()

            available_index.status = ProvisionerStatusEnum.ASSIGNED

            knowledge_base = KnowledgeBase(
                user_id=arg.user_id,
                index_id=available_index.id,
                name=arg.name,
            )

            db.add(knowledge_base)

        await db.refresh(knowledge_base)

        return knowledge_base

    except NoResultFound:
        await db.rollback()
        raise RuntimeError("no available vector indexes found")

    except IntegrityError as e:
        await db.rollback()
        raise RuntimeError(
            f"database integrity error while creating knowledge base: {str(e)}"
        )

    except Exception as e:
        await db.rollback()
        raise RuntimeError(f"failed to create knowledge base in database: {str(e)}")


async def list_users_kb(
    *,
    db: AsyncSession,
    limit: int = 100,
    offset: int = 0,
    user_id: int,
) -> Tuple[List[KnowledgeBase], int]:
    stmt = select(KnowledgeBase).where(KnowledgeBase.user_id == user_id)

    count_stmt = select(func.count()).select_from(stmt.subquery())

    result = await db.execute(count_stmt)
    total_count = result.scalar()

    stmt = stmt.limit(limit=limit)
    stmt = stmt.offset(offset=offset)

    kb_result = await db.execute(stmt)

    kb = kb_result.scalars().all()

    return kb, total_count


async def list_kb_docs(
    *,
    db: AsyncSession,
    limit: int = 200,
    offset: int = 0,
    user_id: int,
    kb_id: int,
) -> ListedKbDocs:

    query = (
        select(
            DocumentRegistry.id,
            DocumentRegistry.file_name,
            KnowledgeBaseDocument.id.label("kb_doc_id"),
            KnowledgeBaseDocument.status,
        )
        .join(
            KnowledgeBaseDocument,
            DocumentRegistry.id == KnowledgeBaseDocument.document_id,
        )
        .where(
            not DocumentRegistry.lock_status,
            DocumentRegistry.user_id == user_id,
            DocumentRegistry.op_status == OperationStatusEnum.SUCCESS,
            KnowledgeBaseDocument.knowledge_base_id == kb_id,
        )
    )

    count_stmt = select(func.count()).select_from(query.subquery())

    count_result = await db.execute(count_stmt)

    total_count = count_result.scalar()

    query = query.limit(limit=limit)
    query = query.offset(offset=offset)

    result = await db.execute(query)

    docs = [
        KbDoc(
            doc_id=row.id,
            kb_doc_id=row.kb_doc_id,
            file_name=row.file_name,
            status=row.status.value,
        )
        for row in result.all()
    ]

    return ListedKbDocs(kb_docs=docs, total_count=total_count, knowledge_base_id=kb_id)


async def delete_kb_db(*, db: AsyncSession, user_id: int, kb_id: int) -> bool:
    try:
        async with db.begin():
            stmt = (
                select(KnowledgeBase)
                .options(selectinload(KnowledgeBase.vector_index))
                .where(KnowledgeBase.id == kb_id, KnowledgeBase.user_id == user_id)
            )
            result = await db.execute(stmt)
            kb = result.scalar_one()

            if kb.vector_index:
                kb.vector_index = ProvisionerStatusEnum.CLEANUP
            else:
                raise RuntimeError(
                    f"inconsistent state: KnowledgeBase {kb_id} has no association vector index"
                )
            await db.execute(
                delete(KnowledgeBaseDocument).where(
                    KnowledgeBaseDocument.knowledge_base_id == kb_id
                )
            )
            await db.delete(kb)
        return True
    except NoResultFound:
        raise
    except Exception:
        await db.rollback()
        raise
