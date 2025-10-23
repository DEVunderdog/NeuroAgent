from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class FileForIngestion(BaseModel):
    kb_doc_id: int
    doc_id: int
    file_name: str
    object_key: Optional[str] = None

class SqsMessage(BaseModel):
    ingestion_job_id: int
    index_kb_doc_id: Optional[List[FileForIngestion]] = None
    delete_kb_doc_id: Optional[List[FileForIngestion]] = None
    index_arn: str
    kb_id: int
    user_id: int


class ReceivedSqsMessage(BaseModel):
    message_id: str
    receipt_handle: str
    body: SqsMessage
    attributes: Optional[Dict[str, Any]] = None
    message_attributes: Optional[Dict[str, Any]] = None


class IngestVectorsParams(BaseModel):
    vectorBucketName: str
    indexArn: str
    vectors: List[float]
    metadata: Dict[str, str]

