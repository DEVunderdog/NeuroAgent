import uuid
from fastapi import APIRouter, status, HTTPException
from typing import List, Dict
from app.api.deps import SessionDep, TokenPayloadDep, AwsDep
from app.models.api import (
    GeneratedPresignedUrls,
    GeneratePresignedUrlsReq,
    GeneratedUrls,
    StandardResponse,
    FinalizeDocumentReq,
    ListDocuments,
    Document,
)
from app.models.database import CreateDocument
from app.database.document_registry import (
    create_document,
    finalize_documents,
    list_files,
    lock_documents,
    DocumentInKnowledgeBaseError,
    delete_documents,
)
from app.aws.client import FileNotSupported, ClientError

import logging

router = APIRouter(prefix="/documents", tags=["Documents"])

logger = logging.getLogger(__name__)


@router.post(
    "/upload",
    response_model=GeneratedPresignedUrls,
    status_code=status.HTTP_201_CREATED,
    summary="generate presigned urls for documents that needs to be uploaded",
)
async def upload_documents(
    req: GeneratePresignedUrlsReq,
    db: SessionDep,
    payload: TokenPayloadDep,
    aws_client: AwsDep,
):
    try:
        if len(req.files) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="please provide files to generate presigned URLs",
            )
        list_of_documents: List[CreateDocument] = []
        url_by_filename: Dict[str, str] = {}

        for file in req.files:
            unique_id = uuid.uuid4()
            filename = f"{unique_id}-{file}"
            object_key = f"{payload.user_id}/{filename}"
            content_type = aws_client.extract_content_type(filename=file)
            url = aws_client.generate_presigned_upload_url(
                object_key=object_key, content_type=content_type
            )
            url_by_filename[filename] = url
            document = CreateDocument(
                user_id=payload.user_id, file_name=filename, object_key=object_key
            )
            list_of_documents.append(document)

        created_documents = await create_document(db=db, docs=list_of_documents)
        final_response: List[GeneratedPresignedUrls] = []

        for doc_id, filename in created_documents:
            presigned_url = url_by_filename.get(filename)
            if presigned_url:
                generated_urls = GeneratedUrls(
                    id=doc_id,
                    url=presigned_url,
                )
                final_response.append(generated_urls)
            else:
                logger.error(
                    f"Consistency Error: Could not find pre-signed URL for document ID {doc_id} "
                    f"with filename '{filename}'. This record will be an orphan."
                )

        return GeneratedPresignedUrls(
            message="generated presigned urls successfully", urls=final_response
        )
    except FileNotSupported as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ClientError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"an exception occurred: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="error uploading document",
        )


@router.put(
    "/finalize",
    response_model=StandardResponse,
    status_code=status.HTTP_200_OK,
    summary="finalized failed and successful files",
)
async def post_upload_documents(req: FinalizeDocumentReq, db: SessionDep):
    if len(req.failed) == 0 and len(req.successful) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide documents to finalize",
        )

    try:
        await finalize_documents(db=db, successful=req.successful, failed=req.failed)
        return StandardResponse(message="successfully finalized the documents")
    except Exception as e:
        logger.exception(f"error finalizing document: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="error finalizing documents",
        )


@router.get(
    "/list",
    response_model=ListDocuments,
    status_code=status.HTTP_200_OK,
    summary="list of documents",
)
async def list_documents(
    db: SessionDep, payload: TokenPayloadDep, limit: int = 100, offset: int = 0
):
    try:
        db_documents, total_count = await list_files(
            db=db, user_id=payload.user_id, limit=limit, offset=offset
        )

        if len(db_documents) == 0:
            return ListDocuments(
                documents=[], total_count=total_count, message="none documents found"
            )
        response_documents = [Document.model_validate(doc) for doc in db_documents]

        return ListDocuments(
            documents=response_documents,
            total_count=total_count,
            message="successfully fetched documents",
        )

    except Exception as e:
        logger.exception(f"error listing documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="error listing documents",
        )


@router.delete(
    "/delete/{file_id}",
    response_model=StandardResponse,
    status_code=status.HTTP_200_OK,
    summary="delete documents",
)
async def delete_file(
    db: SessionDep, payload: TokenPayloadDep, aws_client: AwsDep, file_id: int
):
    if file_id == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please provide valid file id to delete",
        )

    ids = [file_id]
    try:
        object_keys = await lock_documents(
            db=db, document_ids=ids, user_id=payload.user_id
        )

        if len(object_keys) == 0:
            msg = "non documents found"
            logger.info(msg)
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)

        aws_client.multiple_delete_objects(object_keys=object_keys)

        await delete_documents(db=db, document_ids=ids, user_id=payload.user_id)

    except DocumentInKnowledgeBaseError:
        msg = "cannot delete file: it is currently part of knowledge base"
        logger.warning(f"{msg} FILE ID: {file_id}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=msg,
        )

    except HTTPException:
        raise

    except Exception as e:
        msg = f"error deleting documents, please sync up: {str(e)}"
        logger.error(msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=msg,
        )
    
    return StandardResponse(message="successfully deleted files")
