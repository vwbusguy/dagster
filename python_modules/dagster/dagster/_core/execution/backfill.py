from abc import ABC, abstractmethod
from enum import Enum

from dagster._serdes import whitelist_for_serdes


@whitelist_for_serdes
class BulkActionStatus(Enum):
    REQUESTED = "REQUESTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"

    @staticmethod
    def from_graphql_input(graphql_str):
        return BulkActionStatus(graphql_str)


class IBackfill(ABC):
    @property
    @abstractmethod
    def backfill_id(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def status(self) -> BulkActionStatus:
        raise NotImplementedError()

    @property
    @abstractmethod
    def backfill_timestamp(self) -> float:
        raise NotImplementedError()

    @abstractmethod
    def with_status(self, status: BulkActionStatus) -> "IBackfill":
        raise NotImplementedError()

    @abstractmethod
    def execute_backfill_iteration():
        ...
