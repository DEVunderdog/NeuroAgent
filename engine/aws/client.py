import boto3
import logging
import json
from typing import Optional, List
from pydantic import ValidationError
from botocore.exceptions import ClientError
from engine.utils.config import Settings
from engine.models.aws import ReceivedSqsMessage, SqsMessage, IngestVectorsParams


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

class SqsMessageError(SqsOperationError):
    pass

class AwsClientManager:
    def __init__(self, settings: Settings):
        self.settings = settings
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

    def download_file(self, object_key: str, temp_file_path: str):
        try:
            self.s3.download_file(
                self.settings.AWS_BUCKET_NAME, object_key, temp_file_path
            )
            logger.debug(f"download the file: {object_key}")
        except ClientError as e:
            logger.error("error downloading file", extra={"error": str(e)})
            raise S3OperationError(
                f"error downloading file: {str(e)}", object_key=object_key
            )

    def receive_sqs_message(
        self,
        max_messages: int = 5,
        wait_time_seconds: int = 10,
        message_attribute_names: Optional[List[str]] = None,
    ) -> List[ReceivedSqsMessage]:
        try:
            params = {
                "QueueUrl": self.settings.AWS_QUEUE_URL,
                "MaxNumberOfMessages": min(max_messages, 10),
                "WaitTimeSeconds": min(wait_time_seconds, 20),
            }

            if message_attribute_names:
                params["MessageAttributeNames"] = message_attribute_names
            else:
                params["MessageAttributeNames"] = ["All"]

            response = self.sqs.receive_message(**params)
            messages = response.get("Messages", [])

            parsed_messages = []
            for raw_msg in messages:
                try:
                    message_body = json.loads(raw_msg["Body"])
                    sqs_message = SqsMessage.model_validate(message_body)

                    received_message = ReceivedSqsMessage(
                        message_id=raw_msg["MessageId"],
                        receipt_handle=raw_msg["ReceiptHandle"],
                        body=sqs_message,
                        attributes=raw_msg.get("Attributes"),
                        message_attributes=raw_msg.get("MessageAttributes"),
                    )
                    parsed_messages.append(received_message)
                except (json.JSONDecodeError, ValidationError):
                    logger.error("failed to parse sqs message")
                    continue
            return parsed_messages
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(f"failed to receive messages: {error_message}")
            raise SqsMessageError(
                f"failed to receive messages: {error_message}", error_code=error_code
            )
        except Exception as e:
            logger.error(f"unexpected error receiving messages: {e}")
            raise SqsMessageError(f"unexpected error: {e}")

    def delete_message(self, receipt_handle: str) -> bool:
        try:
            self.sqs.delete_message(
                QueueUrl=self.settings.AWS_QUEUE_URL, ReceiptHandle=receipt_handle
            )
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(f"failed to delete message: {error_message}")
            raise SqsMessageError(
                f"failed to delete message: {error_message}", error_code=error_code
            )
        except Exception as e:
            logger.error(f"unexpected error deleting message: {e}")
            raise SqsMessageError(f"unexpected error: {e}")

    def ingest_vectors(self, args: IngestVectorsParams):
        try:
            self.s3_vectors.put_vectors(
                vectorBucketName=args.vectorBucketName,
                indexArn=args.indexArn,
                vectors=[
                    {
                        "key": "text_vector",
                        "data": {"float32": args.vectors},
                        "metadata": args.metadata,
                    },
                ],
            )
        except Exception as e:
            logger.error(f"unexepcted error while ingesting vectors: {e}")
            raise