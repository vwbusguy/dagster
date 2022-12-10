"""
To consider
- Multiple backfills target the same asset
- Parent assets not included in the backfill
- Failure
- Failure with transitive dependencies
- Two parents
- Two children
- Asset has two parents and one succeeds and one fails
- Asset has two parents and one is materialized and the other isn't
- Materialization happens to one of the asset partitions in the backfill, outside of the backfill
- Asset has two parents and only one is in the backfill
- Self-deps
- Non-partitioned assets
- Partition mappings
- Multi-assets
- Reconciliation sensor should avoid targeting targets of ongoing backfills
"""
from typing import AbstractSet, Dict, NamedTuple, Optional, Sequence, Set, Union, cast

import pytest
from dagster_tests.core_tests.definitions_tests.test_asset_reconciliation_sensor import (
    RunSpec,
    do_run,
    one_asset_daily_partitions,
    one_asset_one_partition,
    one_asset_two_partitions,
    two_assets_in_sequence_fan_in_partitions,
    two_assets_in_sequence_fan_out_partitions,
    two_assets_in_sequence_one_partition,
)

from dagster import (
    AssetKey,
    AssetSelection,
    AssetsDefinition,
    DagsterInstance,
    PartitionsDefinition,
    RunRequest,
    SourceAsset,
)
from dagster._core.definitions.asset_backfills import (
    AssetBackfill,
    AssetBackfillCursor,
    execute_asset_backfill_iteration,
)
from dagster._core.definitions.asset_graph import AssetGraph
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._core.definitions.partition import PartitionsSubset


class AssetBackfillScenario(NamedTuple):
    assets: Sequence[Union[SourceAsset, AssetsDefinition]]
    unevaluated_runs: Sequence[RunSpec] = []
    expected_run_requests: Optional[Sequence[RunRequest]] = None
    expected_iterations: Optional[int] = None


# scenarios = dict(
#     one_asset_one_partition=AssetBackfillScenario(assets=one_asset_one_partition),
#     one_asset_two_partitions=AssetBackfillScenario(assets=one_asset_two_partitions),
#     two_assets_two_partitions=AssetBackfillScenario(assets=two_assets_two_partitions),
# )


@pytest.mark.parametrize("some_or_all", ["all", "some"])
@pytest.mark.parametrize("failures", ["no_failures", "root_failures", "random_half_failures"])
@pytest.mark.parametrize(
    "assets_var",
    [
        "one_asset_one_partition",
        "one_asset_two_partitions",
        "two_assets_in_sequence_one_partition",
        "two_assets_in_sequence_fan_in_partitions",
        "two_assets_in_sequence_fan_out_partitions",
        # one_asset_daily_partitions,
    ],
)
def test_scenario_to_completion(assets_var: str, failures: str, some_or_all: str):
    assets = globals()[assets_var]

    asset_graph = AssetGraph.from_assets(assets)
    asset_keys = (asset_def.key for asset_def in assets)
    root_asset_keys = AssetSelection.keys(*asset_keys).sources().resolve(asset_graph)

    if some_or_all == "all":
        target_subsets_by_asset_key: Dict[AssetKey, PartitionsSubset] = {}
        for asset_def in assets:
            partitions_def = cast(PartitionsDefinition, asset_def.partitions_def)
            target_subsets_by_asset_key[
                asset_def.key
            ] = partitions_def.empty_subset().with_partition_keys(
                partitions_def.get_partition_keys()
            )
    elif some_or_all == "some":
        # all partitions downstream of half of the partitions in each root asset
        root_asset_partitions: Set[AssetKeyPartitionKey] = set()
        for root_asset_key in root_asset_keys:
            partitions_def = asset_graph.get_partitions_def(root_asset_key)
            assert partitions_def is not None
            partition_keys = list(partitions_def.get_partition_keys())
            start_index = len(partition_keys) // 2
            chosen_partition_keys = partition_keys[start_index:]
            root_asset_partitions.update(
                AssetKeyPartitionKey(root_asset_key, partition_key)
                for partition_key in chosen_partition_keys
            )

        target_asset_partitions = asset_graph.bfs_filter_asset_partitions(
            lambda _a, _b: True, root_asset_partitions
        )

        target_subsets_by_asset_key: Dict[AssetKey, PartitionsSubset] = {}
        for asset_key, partition_key in target_asset_partitions:
            assert partition_key is not None
            partitions_def = asset_graph.get_partitions_def(asset_key)
            assert partitions_def is not None
            subset = target_subsets_by_asset_key.get(asset_key, partitions_def.empty_subset())
            target_subsets_by_asset_key[asset_key] = subset.with_partition_keys([partition_key])

    else:
        assert False

    if failures == "no_failures":
        fail_asset_partitions: Set[AssetKeyPartitionKey] = set()
    elif failures == "root_failures":
        fail_asset_partitions: Set[AssetKeyPartitionKey] = {
            AssetKeyPartitionKey(root_asset_key, partition_key)
            for root_asset_key in root_asset_keys
            for partition_key in target_subsets_by_asset_key[root_asset_key].get_partition_keys()
        }
    elif failures == "random_half_failures":
        fail_asset_partitions: Set[AssetKeyPartitionKey] = {
            AssetKeyPartitionKey(asset_key, partition_key)
            for asset_key, subset in target_subsets_by_asset_key.items()
            for partition_key in subset.get_partition_keys()
            if hash(str(asset_key) + partition_key) % 2 == 0
        }

    else:
        assert False

    backfill = AssetBackfill("backfillid_x", target_subsets_by_asset_key)
    run_backfill_to_completion(assets, backfill, fail_asset_partitions)


def run_backfill_to_completion(
    assets, backfill: AssetBackfill, fail_asset_partitions: AbstractSet[AssetKeyPartitionKey]
) -> None:
    # TODO: backfill both all and a subset
    asset_graph = AssetGraph.from_assets(assets)

    iteration_count = 0
    instance = DagsterInstance.ephemeral()

    # assert each asset partition only targeted once
    requested_asset_partitions: Set[AssetKeyPartitionKey] = set()

    # TODO: something should change every time that runs complete
    cursor = AssetBackfillCursor.empty()
    while not backfill.is_complete(cursor):
        result1 = execute_asset_backfill_iteration(
            backfill=backfill,
            asset_graph=asset_graph,
            instance=instance,
            cursor=cursor,
        )
        # iteration_count += 1
        assert result1.cursor != cursor
        print(f"result1 cursor: {result1.cursor}")
        print(f"result1 run requests: {result1.run_requests}")

        # if nothing changes, nothing should happen in the iteration
        result2 = execute_asset_backfill_iteration(
            backfill=backfill,
            asset_graph=asset_graph,
            instance=instance,
            cursor=result1.cursor,
        )
        assert result2.cursor == result1.cursor
        assert result2.run_requests == []

        cursor = result2.cursor

        for run_request in result1.run_requests:
            for asset_key in run_request.asset_selection:
                asset_partition = AssetKeyPartitionKey(asset_key, run_request.partition_key)
                assert (
                    asset_partition not in requested_asset_partitions
                ), f"{asset_partition} requested twice. Requested: {requested_asset_partitions}"
                requested_asset_partitions.add(asset_partition)
                # TODO: assert that partitions downstream of failures are not requested

            do_run(
                all_assets=assets,
                asset_keys=run_request.asset_selection,
                partition_key=run_request.partition_key,
                instance=instance,
                failed_asset_keys=[
                    asset_key
                    for asset_key in run_request.asset_selection
                    if AssetKeyPartitionKey(asset_key, run_request.partition_key)
                    in fail_asset_partitions
                ],
                tags=run_request.tags,
            )

    # TODO: expected iterations
    # if there are no non-identify partiion mappings, the number of iterations should be the number
    # of partitions
    # if scenario.expected_iterations:
    #     assert iteration_count == scenario.expected_iterations
