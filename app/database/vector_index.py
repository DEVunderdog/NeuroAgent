import logging
from datetime import timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case
from schema.schema import VectorIndex, ProvisionerStatusEnum
from app.models.api import PoolStats
from app.utils.application_timezone import get_current_time
from app.constants.globals import TIME_THRESHOLD

logger = logging.getLogger(__name__)


async def get_index_pool_stats(*, db: AsyncSession, time_threshold: bool=True) -> PoolStats:
    try:
        stmt = None
        if time_threshold:
            stmt = select(
                func.count(VectorIndex.id).label("total"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.AVAILABLE),
                            1,
                        ),
                        else_=0,
                    )
                ).label("available_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.PROVISIONING),
                            1,
                        ),
                        else_=0,
                    )
                ).label("provisioning_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.FAILED),
                            1,
                        ),
                        else_=0,
                    )
                ).label("failed_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.CLEANUP),
                            1,
                        ),
                        else_=0,
                    )
                ).label("cleanup_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.DESTROYED),
                            1,
                        ),
                        else_=0,
                    )
                ).label("destroyed_count"),
            )
        else:
            current_time_threshold = get_current_time() - timedelta(
                minutes=TIME_THRESHOLD
            )
            stmt = select(
                func.count(VectorIndex.id).label("total"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.AVAILABLE),
                            1,
                        ),
                        else_=0,
                    )
                ).label("available_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.PROVISIONING)
                            & (VectorIndex.created_at >= current_time_threshold),
                            1,
                        ),
                        else_=0,
                    )
                ).label("provisioning_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.FAILED),
                            1,
                        ),
                        else_=0,
                    )
                ).label("failed_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.CLEANUP),
                            1,
                        ),
                        else_=0,
                    )
                ).label("cleanup_count"),
                func.sum(
                    case(
                        (
                            (VectorIndex.status == ProvisionerStatusEnum.DESTROYED),
                            1,
                        ),
                        else_=0,
                    )
                ).label("destroyed_count"),
            )

        counts = (await db.execute(stmt)).one()

        available_count = counts.available_count or 0
        provisioning_count = counts.provisioing_count or 0
        failed_count = counts.failed_count or 0
        cleanup_count = counts.cleanup_count or 0
        destroyed_count = counts.destroyed_count or 0

        return PoolStats(
            message="successfully fetched pool stats",
            available_count=available_count,
            provisioning_count=provisioning_count,
            failed_count=failed_count,
            cleanup_count=cleanup_count,
            destroyed_count=destroyed_count,
        )

    except Exception as e:
        logger.error(f"error getting pool collection status: {e}")
        raise
