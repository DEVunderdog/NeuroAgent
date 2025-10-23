import logging
from fastapi import APIRouter, status, HTTPException
from app.models.api import (
    KnowledgeBaseReq,
    KnowledgeBaseResp,
    ListKnowledgeBaseResp,
    ListedKbDocResp,
    StandardResponse,
)
from app.models.database import CreateKbParams
from app.database.knowledge_base import (
    create_kb_db,
    list_users_kb,
    list_kb_docs,
    delete_kb_db,
)
from app.api.deps import ProvisionerDep, TokenPayloadDep, SessionDep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/kb", tags=["Knowledge Base"])


@router.post(
    "/create",
    response_model=KnowledgeBaseResp,
    status_code=status.HTTP_201_CREATED,
    summary="create knowledge base",
)
async def create_knowledge_base(
    req: KnowledgeBaseReq,
    db: SessionDep,
    provisioner: ProvisionerDep,
    payload: TokenPayloadDep,
):
    try:
        args = CreateKbParams(
            user_id=payload.user_id,
            name=req.name,
        )
        created_kb = await create_kb_db(db=db, arg=args)
        provisioner.trigger_reconciliation()
        return KnowledgeBaseResp(
            message="successfully created knowledge base", kb_id=created_kb.id
        )
    except Exception as e:
        msg = f"an exception occurred while creating knowledge base: {str(e)}"
        logger.error(msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=msg,
        )


@router.get(
    "/list",
    response_model=ListKnowledgeBaseResp,
    status_code=status.HTTP_200_OK,
    summary="list of knowledge base",
)
async def list_kb(
    db: SessionDep, payload: TokenPayloadDep, limit: int = 100, offset: int = 0
):
    listed_kb, total_count = list_users_kb(
        db=db, limit=limit, offset=offset, user_id=payload.user_id
    )

    return ListKnowledgeBaseResp(
        kb=listed_kb,
        total_count=total_count,
        message="successfully fetched knowledge base",
    )


@router.get(
    "/docs/list",
    response_model=ListedKbDocResp,
    status_code=status.HTTP_200_OK,
    summary="list knowledge base documents",
)
async def list_knowledge_base_docs(
    db: SessionDep,
    payload: TokenPayloadDep,
    kb_id: int,
    limit: int = 100,
    offset: int = 0,
):
    try:
        if kb_id == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="please provide knowledge base id to list knowledge base documents",
            )
        result = await list_kb_docs(
            db=db,
            limit=limit,
            offset=offset,
            user_id=payload.user_id,
            kb_id=kb_id,
        )

        return ListedKbDocResp(
            kb_docs=result.kb_docs,
            total_count=result.total_count,
            knowledge_base_id=result.knowledge_base_id,
            message="successfully listed knowledge base documents",
        )

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"exception while fetching knowledge base docs: {str(e)}",
        )


@router.delete(
    "/delete/{kb_id}",
    response_model=StandardResponse,
    status_code=status.HTTP_200_OK,
    summary="delete knowledge base",
)
async def delete_kb(
    db: SessionDep,
    payload: TokenPayloadDep,
    provisioner: ProvisionerDep,
    kb_id: int,
):
    if kb_id == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide knowledge base id to delete",
        )

    try:
        result = await delete_kb_db(db=db, user_id=payload.user_id, kb_id=kb_id)
        provisioner.trigger_cleanup()

        if result:
            return StandardResponse(message="successfully deleted knowledge base")
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="cannot find knowledge base to delete",
            )

    except HTTPException:
        raise
    except Exception as e:
        msg = "an exception occurred while deleting knowledge base"
        logger.error(msg, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )
