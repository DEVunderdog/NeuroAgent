import json
import boto3
import logging
import os
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError
from typing import Optional, List, Dict, Any
from app.utils.config import Settings
from app.constants.content_type import S3_CONTENT_TYPE_MAP
from app.models.aws import (
    SqsMessage,
    CreateVectorIndexParams,
    DeleteVectorIndexParams,
    QueryVectorsParams,
)

logger = logging.getLogger(__name__)


class S3OperationError(Exception):
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        object_key: Optional[str] = None,
    ):
        self.error_code = error_code
        self.object_key = object_key
        super().__init__(message)


class S3AccessDeniedError(S3OperationError):
    pass


class S3ObjectNotFoundError(S3OperationError):
    pass


class S3ConfigurationError(S3OperationError):
    pass


class FileNotSupported(S3OperationError):
    pass


class SqsOperationError(Exception):
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        queue_name: Optional[str] = None,
    ):
        self.error_code = error_code
        self.queue_name = queue_name
        super().__init__(message)


class SqsConfigurationError(SqsOperationError):
    pass


class SqsMessageError(SqsOperationError):
    pass


class AwsClientManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session_kwargs = {"region_name": self.settings.AWS_REGION}
        self.session_kwargs["aws_access_key_id"] = self.settings.AWS_ACCESS_KEY_ID
        self.session_kwargs["aws_secret_access_key"] = (
            self.settings.AWS_SECRET_ACCESS_KEY
        )
        self.session = boto3.Session(**self.session_kwargs)
        self._s3_client = None
        self._sqs_client = None
        self._s3_vectors_client = None

    @property
    def s3(self):
        if self._s3_client is None:
            self._s3_client = self.session.client("s3")
        return self._s3_client

    @property
    def sqs(self):
        if self._sqs_client is None:
            self._sqs_client = self.session.client(
                "sqs", region_name=self.settings.AWS_REGION
            )
        return self._sqs_client

    @property
    def s3_vectors(self):
        if self._s3_vectors_client is None:
            self._s3_vectors_client = self.session.client("s3vectors")
        return self._s3_vectors_client

    def _handle_client_error(
        self, error: ClientError, operation: str, object_key: Optional[str] = None
    ) -> None:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        error_message = error.response.get("Error", {}).get("Message", str(error))

        logger.error(
            f"S3 {operation} failed - Code: {error_code}, Message: {error_message}, Key: {object_key}"
        )

        if error_code == "AccessDenied":
            raise S3AccessDeniedError(
                f"Access denied for S3 {operation}: {error_message}",
                error_code=error_code,
                object_key=object_key,
            )
        elif error_code == "NoSuchKey" or error_code == "404":
            raise S3ObjectNotFoundError(
                f"S3 object not found during {operation}: {error_message}",
                error_code=error_code,
                object_key=object_key,
            )
        else:
            raise S3OperationError(
                f"S3 {operation} failed: {error_message}",
                error_code=error_code,
                object_key=object_key,
            )

    def generate_presigned_upload_url(
        self, object_key: str, content_type: Optional[str] = None
    ) -> Optional[str]:
        if not self.s3:
            logger.error("s3 client not configured for generating presigned url")
            return None

        try:
            params = {"Bucket": self.settings.AWS_BUCKET_NAME, "Key": object_key}
            if content_type:
                params["ContentType"] = content_type

            response = self.s3.generate_presigned_url(
                "put_object",
                Params=params,
                ExpiresIn=self.settings.AWS_PRESIGNED_URL_EXP,
            )
            return response
        except ClientError as e:
            self._handle_client_error(e, "presigned URL generation", object_key)
        except (NoCredentialsError, BotoCoreError) as e:
            logger.error(f"AWS configuration error during presigned URL generation {e}")
            raise S3ConfigurationError(f"AWS configuration error: {e}")
        except Exception as e:
            logger.error(f"unexpected error generating presigned URL: {e}")
            raise S3OperationError(f"unexpected error generating presigned URL: {e}")

    def extract_content_type(self, filename: str) -> Optional[str]:
        if not filename or not isinstance(filename, str):
            return None

        _, extension = os.path.splitext(filename)

        if not extension:
            return None

        content_type = S3_CONTENT_TYPE_MAP.get(extension.lower())

        if content_type is None:
            raise FileNotSupported(f"{extension} file format is not supported")

        return content_type

    def individual_delete_object(self, object_key: str) -> bool:
        try:
            response = self.s3.delete_object(
                Bucket=self.settings.AWS_BUCKET_NAME, Key=object_key
            )
            status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 204:
                return True
            else:
                error_msg = f"Unexpected status code {status_code} for object deletion: {object_key}"
                logger.error(error_msg)
                raise S3OperationError(error_msg, object_key=object_key)

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.warning(f"Object not found for deletion: {object_key}")
                return True
            self._handle_client_error(e, "individual object deletion", object_key)
        except Exception as e:
            logger.error("error deleting object due to exception", exc_info=True)
            raise S3OperationError(
                f"Unexpected error deleting object: {e}", object_key=object_key
            )

    def multiple_delete_objects(self, object_keys: List[str]):
        try:
            objects_to_delete = [{"Key": key} for key in object_keys]

            response = self.s3.delete_objects(
                Bucket=self.settings.AWS_BUCKET_NAME,
                Delete={"Objects": objects_to_delete, "Quiet": False},
            )

            deleted_objects = response.get("Deleted", [])

            errors = response.get("Errors", [])

            result = {
                "deleted_count": len(deleted_objects),
                "deleted_objects": [obj.get("Key") for obj in deleted_objects],
                "error_count": len(errors),
                "errors": errors,
            }

            logger.info(
                f"Batch deletion completed: {result['deleted_count']} deleted, {result['error_count']} errors"
            )

            if errors:
                error_details = []
                for error in errors:
                    error_msg = f"Key: {error['Key']}, Code: {error['Code']}, Message: {error['Message']}"
                    logger.error(f"Batch deletion error - {error_msg}")
                    error_details.append(error_msg)
                raise S3OperationError(
                    f"S3 batch deletion failed for {len(errors)} objects: {'; '.join(error_details)}"
                )
            return result

        except ClientError as e:
            self._handle_client_error(e, "batch object deletion")
        except S3OperationError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during batch deletion: {e}", exc_info=True)
            raise S3OperationError(f"Unexpected error during batch deletion: {e}")

    def object_exists(self, object_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.settings.AWS_BUCKET_NAME, Key=object_key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404" or error_code == "NoSuchKey":
                return False
            self._handle_client_error(e, "object existence check", object_key)
        except Exception as e:
            logger.error(
                f"unexpected error checking object existence {object_key}: {e}"
            )
            raise S3OperationError(
                f"unexpected error checking object existence: {e}",
                object_key=object_key,
            )

    def _format_message_attributes(
        self, attributes: Dict[str, any]
    ) -> Dict[str, Dict[str, str]]:
        formatted = {}

        for key, value in attributes.items():
            if isinstance(value, str):
                formatted[key] = {"StringValue": value, "DataType": "String"}
            elif isinstance(value, (int, float)):
                formatted[key] = {"StringValue": str(value), "DataType": "String"}
            elif (
                isinstance(value, dict)
                and "StringValue" in value
                and "DataType" in value
            ):
                formatted[key] = value
            else:
                formatted[key] = {
                    "StringValue": json.dumps(value),
                    "DataType": "String",
                }

        return formatted

    def send_sqs_message(
        self,
        message_body: SqsMessage,
        message_attributes: Optional[Dict[str, Any]] = None,
    ):
        try:
            body = message_body.model_dump_json()
            params = {"QueueUrl": self.settings.AWS_QUEUE_URL, "MessageBody": body}

            if message_attributes:
                params["MessageAttributes"] = self._format_message_attributes(
                    message_attributes
                )

            self.sqs.send_message(**params)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(f"failed to send message: {error_message}")
            raise SqsMessageError(
                f"failed to send message: {error_message}", error_code=error_code
            )
        except Exception as e:
            logger.error("unexpected error sending message", exc_info=True)
            raise SqsMessageError(f"unexpected error: {e}")

    def create_vector_index(self, args: CreateVectorIndexParams):
        try:
            self.s3_vectors.create_index(
                vectorBucketArn=args.vector_bucket_arn,
                indexName=args.index_name,
                dataType="float32",
                dimension=args.dimension,
                distanceMetric="cosine",
                metadataConfiguration={
                    "nonFilterableMetadataKeys": args.non_filterable_metadata
                },
            )
        except Exception as e:
            logger.error(f"unexpected error creating vector index: {e}")
            raise

    def delete_vector_index(self, args: DeleteVectorIndexParams):
        try:
            self.s3_vectors.delete_index(
                vectorBucketName=args.vector_bucket_name,
                indexArn=args.index_arn,
            )
        except Exception as e:
            logger.error(f"unexpected error while deleting vector index: {e}")
            raise

    def query_vectors(self, args: QueryVectorsParams):
        try:
            response = self.s3_vectors.query_vectors(
                vectorBucketName=args.vector_bucket_name,
                indexArn=args.index_arn,
                topK=args.topK,
                queryVector={"float32": args.query_vector},
                returnMetadata=True,
                returnDistance=True,
            )
            return response
        except Exception as e:
            logger.error(f"unexpected error while querying vectors: {e}")
            raise

    def list_vector_indexes_count(
        self, vector_bucket_arn: str, max_items: int, page_size: int
    ):
        try:
            paginator = self.s3_vectors.get_paginator("list_indexes")
            page_iterator = paginator.paginate(
                vectorBucketArn=vector_bucket_arn,
                PaginationConfig={
                    "MaxItems": max_items,
                    "PageSize": page_size,
                },
            )

            index_count = 0

            for page in page_iterator:
                for index in page.get("indexes", []):
                    index_count += 1

            return index_count
        except Exception as e:
            logger.error(f"error listing indexes: {e}")
            raise
