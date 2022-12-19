import datetime
import json
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import dagster._check as check
from dagster._core.definitions.asset_graph import AssetGraph
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._core.definitions.logical_version import get_input_event_pointer_tag_key
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.event_api import EventRecordsFilter
from dagster._core.events import DagsterEventType
from dagster._core.storage.event_log import EventLogRecord, SqlEventLogStorage
from dagster._core.storage.event_log.sql_event_log import AssetEventTagsTable
from dagster._core.storage.pipeline_run import (
    IN_PROGRESS_RUN_STATUSES,
    DagsterRun,
    DagsterRunStatus,
    RunRecord,
    RunsFilter,
)
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._core.utils import frozendict
from dagster._utils.cached_method import cached_method
from dagster._utils.merger import merge_dicts

if TYPE_CHECKING:
    from dagster import DagsterInstance

USED_DATA_TAG = ".dagster/used_data"


class CachingInstanceQueryer:
    """Provides utility functions for querying for asset-materialization related data from the
    instance which will attempt to limit redundant expensive calls."""

    def __init__(self, instance: "DagsterInstance"):
        self._instance = instance

        self._latest_materialization_record_cache: Dict[AssetKeyPartitionKey, EventLogRecord] = {}
        # if we try to fetch the latest materialization record after a given cursor and don't find
        # anything, we can keep track of that fact, so that the next time try to fetch the latest
        # materialization record for a >= cursor, we don't need to query the instance
        self._no_materializations_after_cursor_cache: Dict[AssetKeyPartitionKey, int] = {}

    @property
    def instance(self) -> "DagsterInstance":
        return self._instance

    @cached_method
    def failed_in_latest_run(self, asset_key: AssetKey) -> bool:
        # the latest asset record is updated when the run is created
        asset_records = self._instance.get_asset_records([asset_key])
        asset_record = next(iter(asset_records), None)

        # no latest run
        if asset_record is None or asset_record.asset_entry.last_run_id is None:
            return False

        run_id = asset_record.asset_entry.last_run_id
        latest_run_record = self._get_run_record_by_id(run_id=run_id)

        # latest run did not fail
        if (
            latest_run_record is None
            or latest_run_record.pipeline_run.status != DagsterRunStatus.FAILURE
        ):
            return False

        # if asset was still materialized in the run count as success
        latest_materialization = asset_record.asset_entry.last_materialization
        if (
            latest_materialization is not None
            and latest_materialization.run_id == latest_run_record.pipeline_run.run_id
        ):
            return False

        return True

    def is_asset_in_run(self, run_id: str, asset: Union[AssetKey, AssetKeyPartitionKey]) -> bool:
        run = self._get_run_by_id(run_id=run_id)
        if not run:
            check.failed("")

        if isinstance(asset, AssetKeyPartitionKey):
            asset_key = asset.asset_key
            if run.tags.get(PARTITION_NAME_TAG) != asset.partition_key:
                return False
        else:
            asset_key = asset

        return asset_key in self._get_planned_materializations_for_run(run_id=run_id)

    def _get_run_by_id(self, run_id: str) -> Optional[DagsterRun]:
        run_record = self._get_run_record_by_id(run_id=run_id)
        if run_record is not None:
            return run_record.pipeline_run
        return None

    @cached_method
    def _get_run_record_by_id(self, run_id: str) -> Optional[RunRecord]:
        return next(
            iter(self._instance.get_run_records(filters=RunsFilter(run_ids=[run_id]), limit=1)),
            None,
        )

    @cached_method
    def get_planned_materializations_for_run_from_events(
        self, run_id: str
    ) -> AbstractSet[AssetKey]:
        materializations_planned = self._instance.get_records_for_run(
            run_id=run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION_PLANNED
        ).records
        return set(cast(AssetKey, record.asset_key) for record in materializations_planned)

    @cached_method
    def _get_planned_materializations_for_run(self, run_id: str) -> AbstractSet[AssetKey]:
        run = self._get_run_by_id(run_id=run_id)
        if run is None:
            return set()

        if run.asset_selection:
            return run.asset_selection
        else:
            # must resort to querying the event log
            return self.get_planned_materializations_for_run_from_events(run_id=run_id)

    @cached_method
    def _get_current_materializations_for_run(self, run_id: str) -> AbstractSet[AssetKey]:
        materializations = self._instance.get_records_for_run(
            run_id=run_id,
            of_type=DagsterEventType.ASSET_MATERIALIZATION,
        ).records
        return set(cast(AssetKey, record.asset_key) for record in materializations)

    @cached_method
    def _get_materialization_record(
        self,
        asset_partition: AssetKeyPartitionKey,
        after_cursor: Optional[int] = None,
        before_cursor: Optional[int] = None,
    ) -> Optional[EventLogRecord]:
        records = self._instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_partition.asset_key,
                asset_partitions=[asset_partition.partition_key]
                if asset_partition.partition_key
                else None,
                after_cursor=after_cursor,
                before_cursor=before_cursor,
            ),
            ascending=False,
            limit=1,
        )
        return next(iter(records), None)

    def get_latest_materialization_record(
        self,
        asset: Union[AssetKey, AssetKeyPartitionKey],
        after_cursor: Optional[int] = None,
        before_cursor: Optional[int] = None,
    ) -> Optional["EventLogRecord"]:
        if isinstance(asset, AssetKey):
            asset_partition = AssetKeyPartitionKey(asset_key=asset)
        else:
            asset_partition = asset

        # fancy caching only applies to after_cursor
        if before_cursor is not None:
            return self._get_materialization_record(
                asset_partition=asset_partition,
                after_cursor=after_cursor,
                before_cursor=before_cursor,
            )

        if asset_partition in self._latest_materialization_record_cache:
            cached_record = self._latest_materialization_record_cache[asset_partition]
            if after_cursor is None or after_cursor < cached_record.storage_id:
                return cached_record
            else:
                return None
        elif asset_partition in self._no_materializations_after_cursor_cache:
            if (
                after_cursor is not None
                and after_cursor >= self._no_materializations_after_cursor_cache[asset_partition]
            ):
                return None

        record = self._get_materialization_record(
            asset_partition=asset_partition, after_cursor=after_cursor, before_cursor=before_cursor
        )

        if record:
            self._latest_materialization_record_cache[asset_partition] = record
            return record
        else:
            if after_cursor is not None:
                self._no_materializations_after_cursor_cache[asset_partition] = min(
                    after_cursor,
                    self._no_materializations_after_cursor_cache.get(asset_partition, after_cursor),
                )
            return None

    def get_latest_materialization_records_by_key(
        self,
        asset_keys: Iterable[AssetKey],
        after_cursor: Optional[int] = None,
        before_cursor: Optional[int] = None,
    ) -> Mapping[AssetKey, "EventLogRecord"]:
        """
        Only returns entries for assets that have been materialized since the cursor.
        """
        result: Dict[AssetKey, "EventLogRecord"] = {}

        for asset_key in asset_keys:
            latest_record = self.get_latest_materialization_record(
                AssetKeyPartitionKey(asset_key),
                after_cursor=after_cursor,
                before_cursor=before_cursor,
            )
            if latest_record is not None:
                result[asset_key] = latest_record

        return result

    @cached_method
    def get_materialization_records(
        self, asset_key: AssetKey, after_cursor: Optional[int] = None
    ) -> Iterable["EventLogRecord"]:
        return self._instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
                after_cursor=after_cursor,
            )
        )

    @cached_method
    def is_reconciled(
        self,
        asset_partition: AssetKeyPartitionKey,
        asset_graph: AssetGraph,
    ) -> bool:
        """
        An asset (partition) is considered unreconciled if any of:
        - It has never been materialized
        - One of its parents has been updated more recently than it has
        - One of its parents is unreconciled
        """
        latest_materialization_record = self.get_latest_materialization_record(
            asset_partition, None
        )

        if latest_materialization_record is None:
            return False

        for parent in asset_graph.get_parents_partitions(
            asset_partition.asset_key, asset_partition.partition_key
        ):
            if (
                self.get_latest_materialization_record(
                    parent, after_cursor=latest_materialization_record.storage_id
                )
                is not None
            ):
                return False

            if not self.is_reconciled(asset_partition=parent, asset_graph=asset_graph):
                return False

        return True

    def set_known_used_data(
        self,
        record: EventLogRecord,
        new_known_data: Dict[AssetKey, Tuple[Optional[int], Optional[float]]],
    ):
        event_log_storage = self._instance.event_log_storage
        if (
            event_log_storage.supports_add_asset_event_tags()
            and record.asset_key is not None
            and record.storage_id is not None
            and record.event_log_entry.timestamp is not None
        ):
            current_known_data = self.get_known_used_data(record.asset_key, record.storage_id)
            known_data = merge_dicts(current_known_data, new_known_data)
            serialized_times = json.dumps(
                {key.to_user_string(): value for key, value in known_data.items()}
            )
            event_log_storage.add_asset_event_tags(
                event_id=record.storage_id,
                event_timestamp=record.event_log_entry.timestamp,
                asset_key=record.asset_key,
                new_tags={USED_DATA_TAG: serialized_times},
            )

    def get_known_used_data(
        self, asset_key: AssetKey, record_id: int
    ) -> Dict[AssetKey, Tuple[Optional[int], Optional[float]]]:
        """Returns the known upstream ids and timestamps stored on the instance"""
        event_log_storage = self._instance.event_log_storage
        if isinstance(event_log_storage, SqlEventLogStorage) and event_log_storage.has_table(
            AssetEventTagsTable.name
        ):
            # attempt to fetch from the instance asset event tags
            tags_list = event_log_storage.get_event_tags_for_asset(
                asset_key=asset_key, filter_event_id=record_id
            )
            if len(tags_list) == 0:
                return {}

            serialized_times = tags_list[0].get(USED_DATA_TAG, "{}")
            return {
                AssetKey.from_user_string(key): tuple(value)  # type:ignore
                for key, value in json.loads(serialized_times).items()
            }
        return {}

    def _upstream_subset(
        self, asset_graph: AssetGraph, start_key: AssetKey, input_keys: AbstractSet[AssetKey]
    ) -> AbstractSet[AssetKey]:
        """Helper method which returns the set up keys in the input set which are upstream of start_key"""
        ret = set()
        if start_key in input_keys:
            ret.add(start_key)
        for upstream_key in asset_graph.upstream_key_iterator(start_key):
            if upstream_key in input_keys:
                ret.add(upstream_key)
        return frozenset(ret)

    @cached_method
    def _calculate_used_data(
        self,
        asset_graph: AssetGraph,
        asset_key: AssetKey,
        record_id: Optional[int],
        record_timestamp: Optional[float],
        record_tags: Mapping[str, str],
        required_keys: AbstractSet[AssetKey],
    ) -> Dict[AssetKey, Tuple[Optional[int], Optional[float]]]:
        if record_id is None:
            return {key: (None, None) for key in required_keys}

        # grab the existing upstream data times already calculated for this record (if any)
        known_data = self.get_known_used_data(asset_key, record_id)
        if asset_key in required_keys:
            known_data[asset_key] = (record_id, record_timestamp)

        # not all required keys have known values
        unknown_required_keys = required_keys - set(known_data.keys())
        if unknown_required_keys:

            # find the upstream times of each of the parents of this asset
            for parent_key in asset_graph.get_parents(asset_key):
                if parent_key in asset_graph.source_asset_keys:
                    continue

                # the set of required keys which are upstream of this parent
                upstream_required_keys = self._upstream_subset(
                    asset_graph, start_key=parent_key, input_keys=unknown_required_keys
                )

                input_event_pointer_tag = get_input_event_pointer_tag_key(parent_key)
                if input_event_pointer_tag in record_tags:
                    # get the upstream materialization event which was consumed when producing this
                    # materialization event
                    input_record_id = int(record_tags[input_event_pointer_tag])
                    parent_record = self.get_latest_materialization_record(
                        parent_key, before_cursor=input_record_id + 1
                    )
                else:
                    # if the input event id was not recorded (materialized pre-1.1.0), just grab
                    # the most recent asset materialization for this parent which happened before
                    # the current record
                    parent_record = self.get_latest_materialization_record(
                        parent_key, before_cursor=record_id
                    )

                # recurse to find the data times of this parent
                for key, tup in self._calculate_used_data(
                    asset_graph=asset_graph,
                    asset_key=parent_key,
                    record_id=parent_record.storage_id if parent_record else None,
                    record_timestamp=parent_record.event_log_entry.timestamp
                    if parent_record
                    else None,
                    record_tags=frozendict(
                        (
                            parent_record.asset_materialization.tags
                            if parent_record and parent_record.asset_materialization
                            else None
                        )
                        or {}
                    ),
                    required_keys=upstream_required_keys,
                ).items():
                    # if root data is missing, this overrides other values
                    if tup == (None, None) or known_data.get(key) == (None, None):
                        known_data[key] = (None, None)
                    else:
                        known_data[key] = min(known_data.get(key, tup), tup)

        return {k: v for k, v in known_data.items() if k in required_keys}

    def get_used_data_times_for_record(
        self,
        asset_graph: AssetGraph,
        record: EventLogRecord,
        upstream_keys: Optional[AbstractSet[AssetKey]] = None,
    ) -> Mapping[AssetKey, Optional[datetime.datetime]]:
        """Method to enable calculating the timestamps of materializations of upstream assets
        which were relevant to a given AssetMaterialization. These timestamps can be calculated relative
        to any upstream asset keys.

        The heart of this functionality is a recursive method which takes a given asset materialization
        and finds the most recent materialization of each of its parents which happened *before* that
        given materialization event.
        """
        if record.asset_key is None or record.asset_materialization is None:
            raise DagsterInvariantViolationError(
                "Can only calculate data times for records with a materialization event and an asset_key."
            )
        if upstream_keys is None:
            upstream_keys = asset_graph.get_non_source_roots(record.asset_key)

        data = self._calculate_used_data(
            asset_graph=asset_graph,
            asset_key=record.asset_key,
            record_id=record.storage_id,
            record_timestamp=record.event_log_entry.timestamp,
            record_tags=frozendict(record.asset_materialization.tags or {}),
            required_keys=frozenset(upstream_keys),
        )
        self.set_known_used_data(record, new_known_data=data)

        return {
            key: datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
            if timestamp is not None
            else None
            for key, (_, timestamp) in data.items()
        }

    @cached_method
    def _get_in_progress_run_ids(self) -> Sequence[str]:
        return [
            record.pipeline_run.run_id
            for record in self._instance.get_run_records(
                filters=RunsFilter(statuses=IN_PROGRESS_RUN_STATUSES), limit=25
            )
        ]

    @cached_method
    def _get_in_progress_data_times_for_key_in_run(
        self,
        run_id: str,
        current_time: datetime.datetime,
        asset_graph: AssetGraph,
        asset_key: AssetKey,
        required_keys: AbstractSet[AssetKey],
    ) -> Mapping[AssetKey, Optional[datetime.datetime]]:
        """Returns the upstream data times that a given asset key will be expected to have at the
        completion of the given run.
        """
        planned_keys = self._get_planned_materializations_for_run(run_id=run_id)
        materialized_keys = self._get_current_materializations_for_run(run_id=run_id)

        # if key is not pending materialization within the run, then downstream pending
        # materializations will (in general) read from the current state of the data
        if asset_key not in planned_keys or asset_key in materialized_keys:
            latest_record = self.get_latest_materialization_record(asset_key)
            latest_used_data = self._calculate_used_data(
                asset_graph=asset_graph,
                asset_key=asset_key,
                record_id=latest_record.storage_id if latest_record else None,
                record_timestamp=latest_record.event_log_entry.timestamp if latest_record else None,
                record_tags=frozendict(
                    (
                        latest_record.asset_materialization.tags
                        if latest_record and latest_record.asset_materialization
                        else None
                    )
                    or {}
                ),
                required_keys=required_keys,
            )
            return {
                key: datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
                if timestamp is not None
                else None
                for key, (_, timestamp) in latest_used_data.items()
            }

        # if you're here, then this asset is planned, but not materialized
        upstream_data_times: Dict[AssetKey, Optional[datetime.datetime]] = {}
        if asset_key in required_keys:
            # in the worst case (data-time wise), this asset gets materialized right now
            upstream_data_times[asset_key] = current_time

        for parent_key in asset_graph.get_parents(asset_key):
            # the set of required keys which are upstream of this parent
            upstream_required_keys = self._upstream_subset(
                asset_graph, start_key=parent_key, input_keys=required_keys
            )
            for upstream_key, upstream_time in self._get_in_progress_data_times_for_key_in_run(
                run_id=run_id,
                current_time=current_time,
                asset_graph=asset_graph,
                asset_key=parent_key,
                required_keys=upstream_required_keys,
            ).items():
                current_value = upstream_data_times.get(upstream_key, upstream_time)
                if current_value is None or upstream_time is None:
                    upstream_data_times[upstream_key] = None
                else:
                    upstream_data_times[upstream_key] = max(current_value, upstream_time)
        return upstream_data_times

    def get_in_progress_data_times_for_key(
        self,
        asset_graph: AssetGraph,
        asset_key: AssetKey,
        upstream_keys: AbstractSet[AssetKey],
        current_time: datetime.datetime,
    ) -> Mapping[AssetKey, datetime.datetime]:
        """Returns a mapping containing the maximum upstream data time that the input asset will
        have once all in-progress runs complete.
        """
        in_progress_times: Dict[AssetKey, datetime.datetime] = {}

        for run_id in self._get_in_progress_run_ids():
            if not self.is_asset_in_run(run_id=run_id, asset=asset_key):
                continue

            for upstream_key, upstream_time in self._get_in_progress_data_times_for_key_in_run(
                run_id=run_id,
                current_time=current_time,
                asset_graph=asset_graph,
                asset_key=asset_key,
                required_keys=upstream_keys,
            ).items():
                if upstream_time is not None:
                    in_progress_times[upstream_key] = max(
                        in_progress_times.get(upstream_key, upstream_time), upstream_time
                    )
        return in_progress_times

    def get_current_minutes_late_for_key(
        self,
        evaluation_time: datetime.datetime,
        asset_graph: AssetGraph,
        asset_key: AssetKey,
    ) -> Optional[float]:
        freshness_policy = asset_graph.freshness_policies_by_key.get(asset_key)
        if freshness_policy is None:
            raise DagsterInvariantViolationError(
                "Cannot calculate minutes late for asset without a FreshnessPolicy"
            )

        latest_record = self.get_latest_materialization_record(asset_key)
        if latest_record is None:
            return None

        used_data_times = self.get_used_data_times_for_record(asset_graph, latest_record)

        return freshness_policy.minutes_late(
            evaluation_time=evaluation_time,
            used_data_times=used_data_times,
            available_data_times={key: evaluation_time for key in used_data_times},
        )
