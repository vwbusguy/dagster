import logging
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    cast,
)

import dagster._check as check
from dagster._core.definitions.asset_reconciliation_sensor import (
    build_run_requests,
    find_parent_materialized_asset_partitions,
)
from dagster._core.definitions.asset_selection import AssetGraph, AssetSelection
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._core.definitions.partition import (
    PartitionsDefinition,
    PartitionsSubset,
    PartitionsSubsetPartitionData,
)
from dagster._core.definitions.run_request import RunRequest
from dagster._core.execution.backfill import BulkActionStatus, IBackfill
from dagster._core.storage.tags import BACKFILL_ID_TAG, PARTITION_NAME_TAG
from dagster._utils.caching_instance_queryer import CachingInstanceQueryer
from dagster._utils.error import SerializableErrorInfo

if TYPE_CHECKING:
    from dagster._core.instance import DagsterInstance


class AssetBackfillCursor(NamedTuple):
    roots_were_requested: bool
    latest_storage_id: Optional[int]
    materialized_subsets_by_asset_key: Mapping[AssetKey, PartitionsSubsetPartitionData]
    failed_subsets_by_asset_key: Mapping[AssetKey, PartitionsSubsetPartitionData]

    def get_materialized_subset(
        self, asset_key: AssetKey, asset_graph: AssetGraph
    ) -> PartitionsSubset:
        return cast(
            PartitionsDefinition, asset_graph.get_partitions_def(asset_key)
        ).get_partitions_subset(self.materialized_subsets_by_asset_key[asset_key])

    def get_failed_subset(self, asset_key: AssetKey, asset_graph: AssetGraph) -> PartitionsSubset:
        return cast(
            PartitionsDefinition, asset_graph.get_partitions_def(asset_key)
        ).get_partitions_subset(self.failed_subsets_by_asset_key[asset_key])

    @classmethod
    def empty(cls) -> "AssetBackfillCursor":
        return cls(
            roots_were_requested=False,
            materialized_subsets_by_asset_key={},
            failed_subsets_by_asset_key={},
            latest_storage_id=None,
        )


class AssetBackfill(
    IBackfill,
    NamedTuple(
        "_PartitionBackfill",
        [
            ("backfill_id", str),
            ("status", BulkActionStatus),
            ("tags", Mapping[str, str]),
            ("backfill_timestamp", float),
            ("error", Optional[SerializableErrorInfo]),
            ("target_subsets_by_asset_key", Mapping[AssetKey, PartitionsSubsetPartitionData]),
            ("cursor", AssetBackfillCursor),
        ],
    ),
):
    """
    A backfill that directly targets software-defined assets, without targeting a job.
    """

    def with_cursor(cursor: AssetBackfillCursor) -> "AssetBackfill":
        return AssetBackfill()

    def is_complete(
        self,
        cursor: AssetBackfillCursor,
        asset_graph: AssetGraph,
    ) -> bool:
        return all(
            len(cursor.get_materialized_subset(asset_key, asset_graph))
            + len(cursor.get_failed_subset(asset_key, asset_graph))
            == len(self.get_target_subset(asset_key, asset_graph))
            for asset_key in self.target_subsets_by_asset_key.keys()
        )

    def get_target_subset(self, asset_key: AssetKey, asset_graph: AssetGraph) -> PartitionsSubset:
        return cast(
            PartitionsDefinition, asset_graph.get_partitions_def(asset_key)
        ).get_partitions_subset(self.target_subsets_by_asset_key[asset_key])

    def is_target(self, asset_partition: AssetKeyPartitionKey, asset_graph: AssetGraph) -> bool:
        return asset_partition.partition_key in self.get_target_subset(
            asset_partition.asset_key, asset_graph
        )

    @property
    def asset_keys(self) -> Iterable[AssetKey]:
        return self.target_subsets_by_asset_key.keys()

    def get_target_root_asset_partitions(
        self, asset_graph: AssetGraph
    ) -> Iterable[AssetKeyPartitionKey]:
        root_asset_keys = AssetSelection.keys(*self.asset_keys).sources().resolve(asset_graph)
        # TODO: this doesn't handle self-deps
        return [
            AssetKeyPartitionKey(asset_key, partition_key)
            for asset_key in root_asset_keys
            for partition_key in self.get_target_subset(asset_key, asset_graph).get_partition_keys()
        ]

    def execute_backfill_iteration(
        self,
        logger: logging.Logger,
        workspace: WorkspaceRequestContext,
        debug_crash_flags: Optional[Mapping[str, int]],
        instance: DagsterInstance,
    ):
        result = execute_asset_backfill_iteration(
            backfill=self, cursor=self.cursor, instance=instance
        )

        backfill_with_cursor = self.with_cursor(result.cursor)

        for run_request in result.run_requests:
            submit_run_request()


class AssetBackfillIterationResult(NamedTuple):
    run_requests: Sequence[RunRequest]
    cursor: AssetBackfillCursor


def execute_asset_backfill_iteration(
    backfill: AssetBackfill,
    cursor: AssetBackfillCursor,
    asset_graph: AssetGraph,
    instance: "DagsterInstance",
) -> AssetBackfillIterationResult:
    print()
    print("STARTING BACKFILL ITERATION")
    instance_queryer = CachingInstanceQueryer(instance=instance)

    initial_candidates: Set[AssetKeyPartitionKey] = set()
    request_roots = not cursor.roots_were_requested
    if request_roots:
        root_asset_partitions = backfill.get_target_root_asset_partitions(asset_graph)
        initial_candidates.update(root_asset_partitions)

    (
        parent_materialized_asset_partitions,
        next_latest_storage_id,
    ) = find_parent_materialized_asset_partitions(
        asset_graph=asset_graph,
        instance_queryer=instance_queryer,
        target_asset_selection=AssetSelection.keys(*backfill.asset_keys),
        latest_storage_id=cursor.latest_storage_id,
    )
    print(f"parent_materialized_asset_partitions: {parent_materialized_asset_partitions}")
    initial_candidates.update(parent_materialized_asset_partitions)

    recently_materialized_partitions_by_asset_key: Mapping[AssetKey, AbstractSet[str]] = {
        asset_key: {
            cast(str, record.partition_key)
            for record in instance_queryer.get_materialization_records(
                asset_key=asset_key, after_cursor=cursor.latest_storage_id
            )
        }
        for asset_key in backfill.asset_keys
        # TODO: filter by backfill tag?
        # TODO: filter by partition?
    }
    print(
        f"recently_materialized_partitions_by_asset_key: {recently_materialized_partitions_by_asset_key}"
    )
    updated_materialized_subsets_by_asset_key: Dict[AssetKey, PartitionsSubset] = {}
    for asset_key in (
        cursor.materialized_subsets_by_asset_key | recently_materialized_partitions_by_asset_key
    ):
        partitions_def = asset_graph.get_partitions_def(asset_key)
        subset = cursor.materialized_subsets_by_asset_key.get(
            asset_key, partitions_def.empty_subset()
        )
        updated_materialized_subsets_by_asset_key[asset_key] = subset.with_partition_keys(
            recently_materialized_partitions_by_asset_key.get(asset_key, [])
        )

    failed_asset_partitions = asset_graph.bfs_filter_asset_partitions(
        lambda asset_partition, _: backfill.is_target(asset_partition, asset_graph),
        _get_failed_asset_partitions(instance_queryer, backfill.backfill_id),
    )

    failed_partitions_by_asset_key = defaultdict(set)
    for asset_key, partition_key in failed_asset_partitions:
        failed_partitions_by_asset_key[asset_key].add(partition_key)

    failed_subsets_by_asset_key = {
        asset_key: asset_graph.get_partitions_def(asset_key)
        .empty_subset()
        .with_partition_keys(partition_keys)
        for asset_key, partition_keys in failed_partitions_by_asset_key.items()
    }

    def handle_candidate(
        candidate: AssetKeyPartitionKey,
        asset_partitions_to_request: AbstractSet[AssetKeyPartitionKey],
    ) -> bool:
        print(f"handling candidate: {candidate}")
        return (
            backfill.is_target(candidate, asset_graph)
            and candidate not in failed_subsets_by_asset_key.get(candidate.asset_key, [])
            and
            # all of its parents materialized first
            all(
                (
                    (
                        parent in asset_partitions_to_request
                        # if they don't have the same partitioning, then we can't launch a run that
                        # targets both, so we need to wait until the parent is reconciled before
                        # launching a run for the child
                        and asset_graph.have_same_partitioning(
                            parent.asset_key, candidate.asset_key
                        )
                    )
                    or parent.partition_key
                    in updated_materialized_subsets_by_asset_key.get(parent.asset_key, [])
                    or not backfill.is_target(parent, asset_graph)
                )
                for parent in asset_graph.get_parents_partitions(
                    candidate.asset_key, candidate.partition_key
                )
            )
            and candidate
            not in updated_materialized_subsets_by_asset_key.get(candidate.asset_key, [])
        )

    asset_partitions_to_request = asset_graph.bfs_filter_asset_partitions(
        handle_candidate, initial_asset_partitions=initial_candidates
    )

    run_requests = build_run_requests(
        asset_partitions_to_request, asset_graph, {BACKFILL_ID_TAG: backfill.backfill_id}
    )
    if request_roots:
        check.invariant(
            asset_partitions_to_request >= set(root_asset_partitions), "all roots are included"
        )

    updated_cursor = AssetBackfillCursor(
        latest_storage_id=next_latest_storage_id or cursor.latest_storage_id,
        roots_were_requested=cursor.roots_were_requested or request_roots,
        materialized_subsets_by_asset_key=updated_materialized_subsets_by_asset_key,
        failed_subsets_by_asset_key=failed_subsets_by_asset_key,
    )
    return AssetBackfillIterationResult(run_requests, updated_cursor)


def _get_failed_asset_partitions(
    instance_queryer: CachingInstanceQueryer, backfill_id: str
) -> Sequence[AssetKeyPartitionKey]:
    """
    Returns asset partitions that materializations were requested for as part of the backfill, but
    will not be materialized.

    Includes canceled asset partitions. Implementation assumes that successful runs won't have any
    failed partitions.
    """
    from dagster._core.storage.pipeline_run import DagsterRunStatus, RunsFilter

    runs = instance_queryer.instance.get_runs(
        filters=RunsFilter(
            tags={BACKFILL_ID_TAG: backfill_id},
            statuses=[DagsterRunStatus.CANCELED, DagsterRunStatus.FAILURE],
        )
    )
    print(f"failed_runs: {runs}")

    result: List[AssetKeyPartitionKey] = []

    for run in runs:
        partition_key = run.tags[PARTITION_NAME_TAG]
        planned_asset_keys = instance_queryer.get_planned_materializations_for_run(
            run_id=run.run_id
        )
        completed_asset_keys = instance_queryer.get_materializations_for_run(run_id=run.run_id)
        result.extend(
            AssetKeyPartitionKey(asset_key, partition_key)
            for asset_key in planned_asset_keys - completed_asset_keys
        )

    print(f"failed_asset_partitions: {result}")

    return result
