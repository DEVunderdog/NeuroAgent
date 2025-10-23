import logging
import asyncio
from datetime import timedelta
from sqlalchemy import and_, or_, select, delete
from app.utils.config import settings
from app.database.connection import SessionLocal
from app.utils.generate import generate_random_string, generate_index_arn
from app.aws.client import AwsClientManager
from schema.schema import VectorIndex, ProvisionerStatusEnum, KnowledgeBase
from app.models.aws import CreateVectorIndexParams, DeleteVectorIndexParams
from app.constants.globals import (
    EMBEDDING_MODEL_DIMENSION,
    NON_FILTERABLE_METADATA_KEY,
    MIN_INDEX_POOL,
    MAX_INDEX_PROVISIONER,
)
from app.utils.application_timezone import get_current_time
from app.database.vector_index import get_index_pool_stats
from app.models.api import PoolStats

logger = logging.getLogger(__name__)


class ProvisionManager:
    def __init__(self, aws_client: AwsClientManager):
        self.aws_client = aws_client
        self.settings = settings
        self.min_pool = MIN_INDEX_POOL
        self.max_provisioner = MAX_INDEX_PROVISIONER

        self._reconcile_trigger_queue = asyncio.Queue()
        self._cleanup_trigger_queue = asyncio.Queue()

    async def provision_new_index(self):
        index_name = f"{generate_random_string}"
        index_arn = generate_index_arn(
            bucket_arn=settings.AWS_VECTOR_BUCKET_ARN, index_name=index_name
        )
        index_record_id = None
        try:
            async with SessionLocal() as db:
                async with db.begin():
                    new_index = VectorIndex(
                        index_arn=index_arn,
                        bucket_arn=settings.AWS_VECTOR_BUCKET_ARN,
                        status=ProvisionerStatusEnum.PROVISIONING,
                    )
                    db.add(new_index)
                await db.refresh(new_index)
                index_record_id = new_index.id
            logger.info(f"successfully initiated vector index creation: {index_name}")
        except Exception as e:
            logger.error(f"error initiating vector index creation: {e}")
            raise

        try:
            args = CreateVectorIndexParams(
                vector_bucket_arn=settings.AWS_VECTOR_BUCKET_ARN,
                index_name=index_name,
                dimension=EMBEDDING_MODEL_DIMENSION,
                non_filterable_metadata=[NON_FILTERABLE_METADATA_KEY],
            )
            await asyncio.to_thread(
                self.aws_client.create_vector_index,
                args=args,
            )
            logger.info(f"successfully created vector index: {index_name}")
        except Exception as e:
            logger.error(f"error creating vector index: {e}")
            try:
                async with SessionLocal() as db:
                    async with db.begin():
                        record_to_delete = await db.get(VectorIndex, index_record_id)
                        if record_to_delete:
                            await db.delete(record_to_delete)
                            logger.info(
                                "successfully rolled back by deleting record for vector index"
                            )
            except Exception as cleanup_e:
                logger.error(
                    f"error deleting initiated vector index in db: {cleanup_e}"
                )
            raise

        try:
            async with SessionLocal() as db:
                async with db.begin():
                    collection_to_update = await db.get(VectorIndex, index_record_id)
                    if not collection_to_update:
                        raise RuntimeError(
                            f"record for vector index id {index_record_id} not found for final update"
                        )
                    collection_to_update.status = ProvisionerStatusEnum.AVAILABLE
                logger.info("successfully provisioned a collection")
        except Exception as e:
            logger.error(f"error finalizing provisioned collection: {e}")
            raise

    async def reconcile_vector_indexes(self):

        pool_stats: PoolStats = None
        async with SessionLocal as db:
            pool_stats = get_index_pool_stats(db=db, time_threshold=True)

        total_count = pool_stats.available_count + pool_stats.provisioning_count

        index_needed = None

        if total_count >= self.min_pool:
            index_needed = 0
        else:
            index_needed = self.min_pool - total_count

        semaphore = asyncio.Semaphore(self.max_provisioner)

        async def provision_with_limit():
            async with semaphore:
                logger.info("dispatching index provisioner task")
                try:
                    await self.provision_new_index()
                    logger.info("successfully provisioned a new vector index")
                except Exception as e:
                    logger.error(f"failed to provision new index: {e}")
                    raise

        exceptions = None
        try:
            async with asyncio.TaskGroup() as tg:
                for i in range(index_needed):
                    tg.create_task(provision_with_limit())
        except* Exception as eg:
            error_msg = (
                f"reconcilation failed during provision of indexes: {eg.exceptions}"
            )
            logger.error(error_msg)
            exceptions = eg

        if exceptions:
            raise

        logger.info("index reconciliation cycle finished")

    async def reconcilation_worker(self):
        logger.info("initial reconcilation cycle finished")
        try:
            await self.reconcile_vector_indexes()
        except Exception as e:
            logger.error(f"initial reconciliation failed, worker will continue : {e}")

        while True:
            try:
                async with asyncio.timeout(300):
                    await self._reconcile_trigger_queue.get()

                    logger.info("event driven trigger received")

                    while not self._reconcile_trigger_queue.empty():
                        self._reconcile_trigger_queue.get_nowait()
                        logger.info("drained a buffered trigger")

            except asyncio.TimeoutError:
                logger.info("starting periodic reconciliation")

            try:
                await self.reconcile_vector_indexes()
            except Exception as e:
                logger.error(f"reconciliation cycle failed with an exception: {e}")

    def trigger_reconciliation(self):
        try:
            self._reconcile_trigger_queue.put_nowait(True)
            logger.info("successfullyy triggered a reconciliation check")
        except asyncio.QueueFull:
            logger.info("reconciliation check is already pending, skipping")

    def trigger_cleanup(self):
        try:
            self._cleanup_trigger_queue.put_nowait(True)
            logger.info("successfully triggered a cleanup")
        except asyncio.QueueFull:
            logger.info("cleanup check is already pending, skipping")

    async def get_cleanup_indexes(self):
        try:
            async with SessionLocal() as db:
                current_time = get_current_time()
                stuck_threshold = current_time - timedelta(minutes=10)

                failed_indexes = VectorIndex.status == ProvisionerStatusEnum.FAILED

                stuck_provisioning_indexes = and_(
                    VectorIndex.status == ProvisionerStatusEnum.PROVISIONING,
                    VectorIndex.created_at < stuck_threshold,
                )

                unlinked_cleanup_indexes = and_(
                    VectorIndex.status == ProvisionerStatusEnum.CLEANUP,
                    KnowledgeBase.vector_index.is_(None),
                )

                stmt = (
                    select(VectorIndex)
                    .outerjoin(KnowledgeBase, VectorIndex.id == KnowledgeBase.index_id)
                    .where(
                        or_(
                            failed_indexes,
                            stuck_provisioning_indexes,
                            unlinked_cleanup_indexes,
                        )
                    )
                    .distinct()
                )

                result = await db.scalars(stmt)
                indexes_to_clean = result.all()

                return indexes_to_clean
        except Exception as e:
            logger.error(
                f"database error while querying for cleanup collection: {e}",
            )
            raise

    async def _cleanup_one_index(self, index: VectorIndex, sem: asyncio.Semaphore):
        async with sem:
            args = DeleteVectorIndexParams(
                vector_bucket_name=settings.AWS_VECTOR_BUCKET_NAME,
                index_arn=index.index_arn,
            )
            try:
                await asyncio.to_thread(self.aws_client.delete_vector_index, args=args)
                logger.info("successfully drop index from bucket")
            except Exception as e:
                logger.error(f"failed to drop index from bucket: {e}")
                raise

            try:
                async with SessionLocal() as db:
                    async with db.begin():
                        delete_stmt = delete(VectorIndex).where(
                            VectorIndex.id == index.id
                        )
                        await db.execute(delete_stmt)
                    logger.info("successfully deleted record for index")
            except Exception as e:
                logger.critical(f"error dropping index record in database: {e}")
                raise

    async def cleanup_indexes(self):
        try:
            indexes_for_cleanup = await self.get_cleanup_indexes()
        except Exception as e:
            logger.error(f"failed to query collection for cleanup: {e}")
            raise

        if len(indexes_for_cleanup) == 0:
            return

        logger.info(f"found {len(indexes_for_cleanup)} collections for cleanup")

        semaphore = asyncio.Semaphore(self.max_provisioner)
        exceptions = None

        try:
            async with asyncio.TaskGroup() as tg:
                for index in indexes_for_cleanup:
                    tg.create_task(self._cleanup_one_index(index=index, sem=semaphore))
        except* Exception as eg:
            error_msg = f"cleanup cycle finished with {len(eg.exceptions)} errors"
            logger.error(error_msg)
            exceptions = eg

        if exceptions:
            logger.info("indexes cleanup cycle finished with errors.")
            raise exceptions
        else:
            logger.info("successfully finished indexes cleanup cycle")

    async def cleanup_worker(self):
        logger.info("event driven cleanup worker for collection")

        while True:
            try:
                await self._cleanup_trigger_queue.get()
                logger.info("event driven trigger received for cleanup collection")

                while not self._cleanup_trigger_queue.empty():
                    self._cleanup_trigger_queue.get_nowait()
                    logger.info("drained cleanup queue buffered trigger")

                await self.cleanup_indexes()

            except Exception as e:
                logger.error(f"cleanup cycle failed with an exception: {e}")
