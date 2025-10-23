import logging
from fastapi import APIRouter, status, HTTPException
from app.api.deps import SessionDep, TokenPayloadDep, AwsDep
from app.models.api import IngestionJobCreationResponse, IngestionRequest
from app.models.database import CreatedIngestionJob
from app.models.aws import SqsMessage
from app.database.ingestion import (
    create_ingestion_job,
    KnowledgebaseNotFound,
    DocsNotFound,
)
from app.aws.client import SqsMessageError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingestion", tags=["data ingestion"])


@router.post(
    "/insert",
    response_model=IngestionJobCreationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="initialize the ingestion of the data from documents",
)
async def ingest_documents(
    req: IngestionRequest, db: SessionDep, payload: TokenPayloadDep, aws_client: AwsDep
):
    doc_ids = req.file_ids or []

    if req.kb_id == 0 or not req.kb_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide knowledge base id to ingestion data",
        )

    if not doc_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide 'file_ids' to start an ingestion job",
        )

    try:
        result: CreatedIngestionJob = await create_ingestion_job(
            db=db,
            document_ids=doc_ids,
            kb_id=req.kb_id,
            user_id=payload.user_id,
        )

        if result.documents:
            message = SqsMessage(
                ingestion_job_id=result.ingestion_id,
                index_arn=result.index_arn,
                index_kb_doc_id=result.documents,
                kb_id=result.kb_id,
                user_id=result.user_id,
            )

            aws_client.send_sqs_message(message_body=message)

        await db.commit()

        return IngestionJobCreationResponse(
            message=f"successfully requested ingestion for {len(result.documents)} documents",
            ingestion_job_id=result.ingestion_id,
        )

    except KnowledgebaseNotFound as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except DocsNotFound as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except SqsMessageError as e:
        await db.rollback()
        logger.error(
            f"sqs message failed after db prep for job. rolling backe: {str(e)}"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"cannot queue ingestion job: {e}",
        )

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        logger.error(
            f"eerror while creating ingestion job for inserting documents: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"error while creating ingestion job for inserting documents: {e}",
        )


@router.delete(
    "/delete",
    response_model=IngestionJobCreationResponse,
    status_code=status.HTTP_200_OK,
    summary="delete the ingested data",
)
async def delete_ingested_data(
    req: IngestionRequest,
    db: SessionDep,
    payload: TokenPayloadDep,
    aws_client: AwsDep,
):
    doc_ids = req.file_ids or []

    if req.kb_id == 0 or not req.kb_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide knowledge base id to delete the ingested data from",
        )

    if not doc_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide 'file_ids' to start an ingestion job",
        )

    try:
        result: CreatedIngestionJob = await create_ingestion_job(
            db=db,
            document_ids=doc_ids,
            kb_id=req.kb_id,
            user_id=payload.user_id,
        )

        if result.documents:
            message = SqsMessage(
                ingestion_job_id=result.ingestion_id,
                delete_kb_doc_id=result.documents,
                index_arn=result.index_arn,
                kb_id=result.kb_id,
                user_id=result.user_id,
            )

            aws_client.send_sqs_message(message_body=message)

        await db.commit()

        return IngestionJobCreationResponse(
            message="successfully requested for deletion of ingested job",
            ingestion_job_id=result.ingestion_id,
        )

    except KnowledgebaseNotFound as e:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    except DocsNotFound as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

    except SqsMessageError as e:
        await db.rollback()
        logger.error(
            f"sqs message failed after db prep for ingestion job: {str(e)}",
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"could not queue ingestion job: {str(e)}",
        )

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        logger.error("error creating ingestion job and sending sqs message")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"an internal error occurred while starting ingestion job: {str(e)}",
        )

