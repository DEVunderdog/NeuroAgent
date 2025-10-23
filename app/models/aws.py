from pydantic import BaseModel
from typing import Optional, List

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

class CreateVectorIndexParams(BaseModel):
    vector_bucket_arn: str
    index_name: str
    dimension: int
    non_filterable_metadata: List[str]

class DeleteVectorIndexParams(BaseModel):
    vector_bucket_name: str
    index_arn: str

class QueryVectorsParams(BaseModel):
    vector_bucket_name: str
    index_arn: str
    topK: int
    query_vector: float
