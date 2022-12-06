from abc import ABC, abstractmethod
from typing import NamedTuple, Optional, Union

import dagster._check as check
from dagster._annotations import experimental, public
from dagster._core.definitions.partition import PartitionsDefinition, PartitionsSubset
from dagster._core.definitions.partition_key_range import PartitionKeyRange
from dagster._core.errors import DagsterInvalidInvocationError
from dagster._serdes import whitelist_for_serdes


@experimental
class PartitionMapping(ABC):
    """Defines a correspondence between the partitions in an asset and the partitions in an asset
    that it depends on.
    """

    @public
    @abstractmethod
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        """Returns the range of partition keys in the upstream asset that include data necessary
        to compute the contents of the given partition key range in the downstream asset.

        Args:
            downstream_partition_key_range (PartitionKeyRange): The range of partition keys in the
                downstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """

    @public
    @abstractmethod
    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        """Returns the range of partition keys in the downstream asset that use the data in the given
        partition key range of the upstream asset.

        Args:
            upstream_partition_key_range (PartitionKeyRange): The range of partition keys in the
                upstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """

    @public
    def get_upstream_partitions_for_partition_subset(
        self,
        downstream_partition_key_subset: Optional[Union[PartitionKeyRange, PartitionsSubset]],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        """
        Returns the subset of partition keys in the upstream asset that include data necessary
        to compute the contents of the given partition key subset in the downstream asset.

        Args:
            downstream_partition_key_subset (Optional[Union[PartitionKeyRange, PartitionsSubset]]):
                The subset of partition keys in the downstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """
        if isinstance(downstream_partition_key_subset, PartitionsSubset):
            raise NotImplementedError(
                "Must be implemented by subclass if passing a PartitionsSubset"
            )
        else:
            upstream_key_range = self.get_upstream_partitions_for_partition_range(
                downstream_partition_key_subset,
                downstream_partitions_def,
                upstream_partitions_def,
            )
            return upstream_partitions_def.empty_subset().with_partition_keys(
                upstream_partitions_def.get_partition_keys_in_range(upstream_key_range)
            )

    @public
    def get_downstream_partitions_for_partition_subset(
        self,
        upstream_partition_key_subset: Union[PartitionKeyRange, PartitionsSubset],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionsSubset:
        """
        Returns the subset of partition keys in the downstream asset that use the data in the given
        partition key subset of the upstream asset.

        Args:
            upstream_partition_key_subset (Union[PartitionKeyRange, PartitionsSubset]): The
                subset of partition keys in the upstream asset.
            downstream_partitions_def (PartitionsDefinition): The partitions definition for the
                downstream asset.
            upstream_partitions_def (PartitionsDefinition): The partitions definition for the
                upstream asset.
        """
        if isinstance(upstream_partition_key_subset, PartitionsSubset):
            raise NotImplementedError(
                "Must be implemented by subclass if passing a PartitionsSubset"
            )
        else:
            downstream_range = self.get_downstream_partitions_for_partition_range(
                upstream_partition_key_subset,
                downstream_partitions_def,
                upstream_partitions_def,
            )
            if downstream_partitions_def is None:
                raise DagsterInvalidInvocationError(
                    "downstream partitions definition must be defined"
                )
            return downstream_partitions_def.empty_subset().with_partition_keys(
                downstream_partitions_def.get_partition_keys_in_range(downstream_range)
            )


@experimental
@whitelist_for_serdes
class IdentityPartitionMapping(PartitionMapping, NamedTuple("_IdentityPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        if downstream_partitions_def is None or downstream_partition_key_range is None:
            check.failed("downstream asset is not partitioned")

        return downstream_partition_key_range

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        return upstream_partition_key_range


@experimental
@whitelist_for_serdes
class AllPartitionMapping(PartitionMapping, NamedTuple("_AllPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        return PartitionKeyRange(
            upstream_partitions_def.get_first_partition_key(),
            upstream_partitions_def.get_last_partition_key(),
        )

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        raise NotImplementedError()


@experimental
@whitelist_for_serdes
class LastPartitionMapping(PartitionMapping, NamedTuple("_LastPartitionMapping", [])):
    def get_upstream_partitions_for_partition_range(
        self,
        downstream_partition_key_range: Optional[PartitionKeyRange],
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        last_partition_key = upstream_partitions_def.get_last_partition_key()
        return PartitionKeyRange(last_partition_key, last_partition_key)

    def get_downstream_partitions_for_partition_range(
        self,
        upstream_partition_key_range: PartitionKeyRange,
        downstream_partitions_def: Optional[PartitionsDefinition],
        upstream_partitions_def: PartitionsDefinition,
    ) -> PartitionKeyRange:
        raise NotImplementedError()


def infer_partition_mapping(
    partition_mapping: Optional[PartitionMapping], partitions_def: Optional[PartitionsDefinition]
) -> PartitionMapping:
    if partition_mapping is not None:
        return partition_mapping
    elif partitions_def is not None:
        return partitions_def.get_default_partition_mapping()
    else:
        return AllPartitionMapping()


def get_builtin_partition_mapping_types():
    from dagster._core.definitions.time_window_partition_mapping import TimeWindowPartitionMapping

    return (
        AllPartitionMapping,
        IdentityPartitionMapping,
        LastPartitionMapping,
        TimeWindowPartitionMapping,
    )
