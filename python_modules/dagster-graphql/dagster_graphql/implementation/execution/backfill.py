from typing import TYPE_CHECKING, Any, Mapping, Optional, Tuple, Union

import pendulum

import dagster._check as check
from dagster._core.definitions.asset_graph import AssetGraph
from dagster._core.errors import DagsterError
from dagster._core.events import AssetKey
from dagster._core.execution.asset_backfill import AssetBackfill, AssetBackfillCursor
from dagster._core.execution.backfill import BulkActionStatus, submit_backfill_runs
from dagster._core.execution.job_backfill import PartitionBackfill
from dagster._core.host_representation import RepositorySelector
from dagster._core.host_representation.external import ExternalPartitionSet
from dagster._core.host_representation.repository_location import RepositoryLocation
from dagster._core.utils import make_new_backfill_id
from dagster._core.workspace.context import WorkspaceRequestContext

from ..utils import capture_error

BACKFILL_CHUNK_SIZE = 25


if TYPE_CHECKING:
    from ...schema.backfill import GrapheneLaunchBackfillSuccess
    from ...schema.errors import GraphenePartitionSetNotFoundError


@capture_error
def create_and_launch_partition_backfill(
    graphene_info, backfill_params: Mapping[str, Any]
) -> Union["GrapheneLaunchBackfillSuccess", "GraphenePartitionSetNotFoundError"]:
    from ...schema.backfill import GrapheneLaunchBackfillSuccess

    backfill_id = make_new_backfill_id()

    external_partition_set, location = _get_external_partition_set_from_backfill_params(
        backfill_params, graphene_info.context
    )

    asset_selection = (
        [AssetKey.from_graphql_input(asset_key) for asset_key in backfill_params["assetSelection"]]
        if backfill_params.get("assetSelection")
        else None
    )

    if external_partition_set is not None:  # job backfill
        if backfill_params.get("allPartitions"):
            result = graphene_info.context.get_external_partition_names(external_partition_set)
            partition_names = result.partition_names
        elif backfill_params.get("partitionNames"):
            partition_names = backfill_params["partitionNames"]
        else:
            raise DagsterError(
                'Backfill requested without specifying either "allPartitions" or "partitionNames" '
                "arguments"
            )

        backfill = PartitionBackfill(
            backfill_id=backfill_id,
            partition_set_origin=external_partition_set.get_external_origin(),
            status=BulkActionStatus.REQUESTED,
            partition_names=partition_names,
            from_failure=bool(backfill_params.get("fromFailure")),
            reexecution_steps=backfill_params.get("reexecutionSteps"),
            tags={t["key"]: t["value"] for t in backfill_params.get("tags", [])},
            backfill_timestamp=pendulum.now("UTC").timestamp(),
            asset_selection=asset_selection,
        )

        if backfill_params.get("forceSynchronousSubmission"):
            # should only be used in a test situation
            to_submit = [name for name in partition_names]
            submitted_run_ids = []

            while to_submit:
                chunk = to_submit[:BACKFILL_CHUNK_SIZE]
                to_submit = to_submit[BACKFILL_CHUNK_SIZE:]
                submitted_run_ids.extend(
                    run_id
                    for run_id in submit_backfill_runs(
                        graphene_info.context.instance,
                        workspace=graphene_info.context,
                        repo_location=location,
                        backfill_job=backfill,
                        partition_names=chunk,
                    )
                    if run_id != None
                )
            return GrapheneLaunchBackfillSuccess(
                backfill_id=backfill_id, launched_run_ids=submitted_run_ids
            )
    elif asset_selection is not None:  # pure asset backfill
        if backfill_params.get("forceSynchronousSubmission"):
            raise DagsterError(
                "forceSynchronousSubmission is not supported for pure asset backfills"
            )

        if backfill_params.get("allPartitions"):
            raise DagsterError("allPartitions is not supported for pure asset backfills")

        partition_names = backfill_params["partitionNames"]

        asset_graph = _get_workspace_asset_graph(graphene_info.context)
        target_subsets_by_asset_key = {
            asset_graph.get_partitions_def(asset_key)
            .empty_subset()
            .with_partition_keys(partition_names)
            .partitions_data
            for asset_key in asset_selection
        }

        backfill = AssetBackfill(
            backfill_id=backfill_id,
            status=BulkActionStatus.REQUESTED,
            cursor=AssetBackfillCursor.empty(),
            target_subsets_by_asset_key=target_subsets_by_asset_key,
        )
    else:
        raise DagsterError(
            "Backfill requested without specifying partition set selector or asset selection"
        )

    graphene_info.context.instance.add_backfill(backfill)
    return GrapheneLaunchBackfillSuccess(backfill_id=backfill_id)


def _get_external_partition_set_from_backfill_params(
    backfill_params: Mapping[str, Any], context: WorkspaceRequestContext
) -> Tuple[Optional[ExternalPartitionSet], Optional[RepositoryLocation]]:
    from ...schema.errors import GraphenePartitionSetNotFoundError

    if "selector" not in backfill_params:
        return None

    partition_set_selector = backfill_params["selector"]
    partition_set_name = partition_set_selector.get("partitionSetName")
    repository_selector = RepositorySelector.from_graphql_input(
        partition_set_selector.get("repositorySelector")
    )
    location = context.get_repository_location(repository_selector.location_name)
    repository = location.get_repository(repository_selector.repository_name)
    matches = [
        partition_set
        for partition_set in repository.get_external_partition_sets()
        if partition_set.name == partition_set_selector.get("partitionSetName")
    ]
    if not matches:
        return GraphenePartitionSetNotFoundError(partition_set_name)

    check.invariant(
        len(matches) == 1,
        "Partition set names must be unique: found {num} matches for {partition_set_name}".format(
            num=len(matches), partition_set_name=partition_set_name
        ),
    )

    return next(iter(matches)), location


@capture_error
def cancel_partition_backfill(graphene_info, backfill_id):
    from ...schema.backfill import GrapheneCancelBackfillSuccess

    backfill = graphene_info.context.instance.get_backfill(backfill_id)
    if not backfill:
        check.failed(f"No backfill found for id: {backfill_id}")

    graphene_info.context.instance.update_backfill(backfill.with_status(BulkActionStatus.CANCELED))
    return GrapheneCancelBackfillSuccess(backfill_id=backfill_id)


@capture_error
def resume_partition_backfill(graphene_info, backfill_id):
    from ...schema.backfill import GrapheneResumeBackfillSuccess

    backfill = graphene_info.context.instance.get_backfill(backfill_id)
    if not backfill:
        check.failed(f"No backfill found for id: {backfill_id}")

    graphene_info.context.instance.update_backfill(backfill.with_status(BulkActionStatus.REQUESTED))
    return GrapheneResumeBackfillSuccess(backfill_id=backfill_id)


def _get_workspace_asset_graph(workspace_request_context: WorkspaceRequestContext) -> AssetGraph:
    repo_locations = (
        location_entry.repository_location
        for location_entry in workspace_request_context.get_workspace_snapshot().values()
        if location_entry.repository_location
    )
    repos = (
        repo
        for repo_location in repo_locations
        for repo in repo_location.get_repositories().values()
    )
    non_source_external_asset_nodes = [
        external_asset_node
        for repo in repos
        for external_asset_node in repo.get_external_asset_nodes()
        if not external_asset_node.is_source
    ]
    return AssetGraph.from_external_assets(non_source_external_asset_nodes)
