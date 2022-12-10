import logging
import os
import sys
import time
from typing import Iterable, Mapping, Optional, Sequence, Tuple, cast

from dagster._core.errors import DagsterBackfillFailedError
from dagster._core.execution.backfill import (
    BulkActionStatus,
    PartitionBackfill,
    submit_backfill_runs,
)
from dagster._core.host_representation.repository_location import RepositoryLocation
from dagster._core.instance import DagsterInstance
from dagster._core.storage.pipeline_run import DagsterRun, RunsFilter
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._core.workspace.context import IWorkspaceProcessContext
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info


def execute_backfill_iteration(
    workspace_process_context: IWorkspaceProcessContext,
    logger: logging.Logger,
    debug_crash_flags: Optional[Mapping[str, int]] = None,
) -> Iterable[Optional[SerializableErrorInfo]]:
    instance = workspace_process_context.instance
    backfills = instance.get_backfills(status=BulkActionStatus.REQUESTED)

    if not backfills:
        logger.debug("No backfill jobs requested.")
        yield None
        return

    workspace = workspace_process_context.create_request_context()

    for backfill_job in backfills:
        backfill_id = backfill_job.backfill_id

        # refetch, in case the backfill was updated in the meantime
        backfill = cast(PartitionBackfill, instance.get_backfill(backfill_id))
        backfill.execute_backfill_iteration(
            backfill, logger, workspace, debug_crash_flags, instance
        )
