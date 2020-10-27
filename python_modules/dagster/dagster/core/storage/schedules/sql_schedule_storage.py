from abc import abstractmethod

import six
import sqlalchemy as db
from dagster import check
from dagster.core.definitions.job import JobType
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.scheduler import ScheduleState, ScheduleTick
from dagster.core.scheduler.job import JobState, JobTick, JobTickData
from dagster.core.scheduler.scheduler import (
    ScheduleTickData,
    ScheduleTickStatsSnapshot,
    ScheduleTickStatus,
)
from dagster.serdes import deserialize_json_to_dagster_namedtuple, serialize_dagster_namedtuple
from dagster.utils import utc_datetime_from_timestamp

from .base import ScheduleStorage
from .schema import JobTable, JobTickTable, ScheduleTable, ScheduleTickTable


class SqlScheduleStorage(ScheduleStorage):
    """Base class for SQL backed schedule storage
    """

    @abstractmethod
    def connect(self):
        """Context manager yielding a sqlalchemy.engine.Connection."""

    def execute(self, query):
        with self.connect() as conn:
            result_proxy = conn.execute(query)
            res = result_proxy.fetchall()
            result_proxy.close()

        return res

    def _deserialize_rows(self, rows):
        return list(map(lambda r: deserialize_json_to_dagster_namedtuple(r[0]), rows))

    def all_stored_schedule_state(self, repository_origin_id=None):
        base_query = db.select(
            [ScheduleTable.c.schedule_body, ScheduleTable.c.schedule_origin_id]
        ).select_from(ScheduleTable)

        if repository_origin_id:
            query = base_query.where(ScheduleTable.c.repository_origin_id == repository_origin_id)
        else:
            query = base_query

        rows = self.execute(query)
        return self._deserialize_rows(rows)

    def get_schedule_state(self, schedule_origin_id):
        check.str_param(schedule_origin_id, "schedule_origin_id")

        query = (
            db.select([ScheduleTable.c.schedule_body])
            .select_from(ScheduleTable)
            .where(ScheduleTable.c.schedule_origin_id == schedule_origin_id)
        )

        rows = self.execute(query)
        return deserialize_json_to_dagster_namedtuple(rows[0][0]) if len(rows) else None

    def get_latest_tick(self, schedule_origin_id):
        check.str_param(schedule_origin_id, "schedule_origin_id")

        query = (
            db.select([ScheduleTickTable.c.id, ScheduleTickTable.c.tick_body])
            .select_from(ScheduleTickTable)
            .where(ScheduleTickTable.c.schedule_origin_id == schedule_origin_id)
            .order_by(ScheduleTickTable.c.timestamp.desc())
            .limit(1)
        )

        rows = self.execute(query)

        if len(rows) == 0:
            return None

        return ScheduleTick(rows[0][0], deserialize_json_to_dagster_namedtuple(rows[0][1]))

    def get_schedule_ticks(self, schedule_origin_id):
        check.str_param(schedule_origin_id, "schedule_origin_id")

        query = (
            db.select([ScheduleTickTable.c.id, ScheduleTickTable.c.tick_body])
            .select_from(ScheduleTickTable)
            .where(ScheduleTickTable.c.schedule_origin_id == schedule_origin_id)
            .order_by(ScheduleTickTable.c.id.desc())
        )

        rows = self.execute(query)
        return list(
            map(lambda r: ScheduleTick(r[0], deserialize_json_to_dagster_namedtuple(r[1])), rows)
        )

    def get_schedule_tick_stats(self, schedule_origin_id):
        check.str_param(schedule_origin_id, "schedule_origin_id")

        query = (
            db.select([ScheduleTickTable.c.status, db.func.count()])
            .select_from(ScheduleTickTable)
            .where(ScheduleTickTable.c.schedule_origin_id == schedule_origin_id)
            .group_by(ScheduleTickTable.c.status)
        )

        rows = self.execute(query)

        counts = {}
        for status, count in rows:
            counts[status] = count

        return ScheduleTickStatsSnapshot(
            ticks_started=counts.get(ScheduleTickStatus.STARTED.value, 0),
            ticks_succeeded=counts.get(ScheduleTickStatus.SUCCESS.value, 0),
            ticks_skipped=counts.get(ScheduleTickStatus.SKIPPED.value, 0),
            ticks_failed=counts.get(ScheduleTickStatus.FAILURE.value, 0),
        )

    def create_schedule_tick(self, schedule_tick_data):
        check.inst_param(schedule_tick_data, "schedule_tick_data", ScheduleTickData)

        with self.connect() as conn:
            try:
                tick_insert = ScheduleTickTable.insert().values(  # pylint: disable=no-value-for-parameter
                    schedule_origin_id=schedule_tick_data.schedule_origin_id,
                    status=schedule_tick_data.status.value,
                    timestamp=utc_datetime_from_timestamp(schedule_tick_data.timestamp),
                    tick_body=serialize_dagster_namedtuple(schedule_tick_data),
                )
                result = conn.execute(tick_insert)
                tick_id = result.inserted_primary_key[0]
                return ScheduleTick(tick_id, schedule_tick_data)
            except db.exc.IntegrityError as exc:
                six.raise_from(
                    DagsterInvariantViolationError(
                        "Unable to insert ScheduleTick for schedule {schedule_name} in storage".format(
                            schedule_name=schedule_tick_data.schedule_name,
                        )
                    ),
                    exc,
                )

    def update_schedule_tick(self, tick):
        check.inst_param(tick, "tick", ScheduleTick)

        with self.connect() as conn:
            conn.execute(
                ScheduleTickTable.update()  # pylint: disable=no-value-for-parameter
                .where(ScheduleTickTable.c.id == tick.tick_id)
                .values(
                    status=tick.status.value,
                    tick_body=serialize_dagster_namedtuple(tick.schedule_tick_data),
                )
            )

        return tick

    def add_schedule_state(self, schedule):
        check.inst_param(schedule, "schedule", ScheduleState)
        with self.connect() as conn:
            try:
                schedule_insert = ScheduleTable.insert().values(  # pylint: disable=no-value-for-parameter
                    schedule_origin_id=schedule.schedule_origin_id,
                    repository_origin_id=schedule.repository_origin_id,
                    status=schedule.status.value,
                    schedule_body=serialize_dagster_namedtuple(schedule),
                )
                conn.execute(schedule_insert)
            except db.exc.IntegrityError as exc:
                six.raise_from(
                    DagsterInvariantViolationError(
                        "ScheduleState {id} is already present "
                        "in storage".format(id=schedule.schedule_origin_id,)
                    ),
                    exc,
                )

        return schedule

    def update_schedule_state(self, schedule):
        check.inst_param(schedule, "schedule", ScheduleState)
        if not self.get_schedule_state(schedule.schedule_origin_id):
            raise DagsterInvariantViolationError(
                "ScheduleState {id} is not present in storage".format(
                    id=schedule.schedule_origin_id
                )
            )

        with self.connect() as conn:
            conn.execute(
                ScheduleTable.update()  # pylint: disable=no-value-for-parameter
                .where(ScheduleTable.c.schedule_origin_id == schedule.schedule_origin_id)
                .values(
                    status=schedule.status.value,
                    schedule_body=serialize_dagster_namedtuple(schedule),
                )
            )

    def delete_schedule_state(self, schedule_origin_id):
        check.str_param(schedule_origin_id, "schedule_origin_id")

        if not self.get_schedule_state(schedule_origin_id):
            raise DagsterInvariantViolationError(
                "ScheduleState {id} is not present in storage".format(id=schedule_origin_id)
            )

        with self.connect() as conn:
            conn.execute(
                ScheduleTable.delete().where(  # pylint: disable=no-value-for-parameter
                    ScheduleTable.c.schedule_origin_id == schedule_origin_id
                )
            )

    def all_stored_job_state(self, repository_origin_id=None, job_type=None):
        check.opt_inst_param(job_type, "job_type", JobType)
        base_query = db.select([JobTable.c.job_body, JobTable.c.job_origin_id]).select_from(
            JobTable
        )

        if repository_origin_id:
            query = base_query.where(JobTable.c.repository_origin_id == repository_origin_id)
        else:
            query = base_query

        if job_type:
            query = query.where(JobTable.c.job_type == job_type.value)

        rows = self.execute(query)
        return self._deserialize_rows(rows)

    def get_job_state(self, job_origin_id):
        check.str_param(job_origin_id, "job_origin_id")

        query = (
            db.select([JobTable.c.job_body])
            .select_from(JobTable)
            .where(JobTable.c.job_origin_id == job_origin_id)
        )

        rows = self.execute(query)
        return self._deserialize_rows(rows[:1])[0] if len(rows) else None

    def add_job_state(self, job):
        check.inst_param(job, "job", JobState)
        with self.connect() as conn:
            try:
                conn.execute(
                    JobTable.insert().values(  # pylint: disable=no-value-for-parameter
                        job_origin_id=job.job_origin_id,
                        repository_origin_id=job.repository_origin_id,
                        status=job.status.value,
                        job_type=job.job_type.value,
                        job_body=serialize_dagster_namedtuple(job),
                    )
                )
            except db.exc.IntegrityError as exc:
                six.raise_from(
                    DagsterInvariantViolationError(
                        "JobState {id} is already present in storage".format(id=job.job_origin_id,)
                    ),
                    exc,
                )

        return job

    def update_job_state(self, job):
        check.inst_param(job, "job", JobState)
        if not self.get_job_state(job.job_origin_id):
            raise DagsterInvariantViolationError(
                "JobState {id} is not present in storage".format(id=job.job_origin_id)
            )

        with self.connect() as conn:
            conn.execute(
                JobTable.update()  # pylint: disable=no-value-for-parameter
                .where(JobTable.c.job_origin_id == job.job_origin_id)
                .values(status=job.status.value, job_body=serialize_dagster_namedtuple(job),)
            )

    def delete_job_state(self, job_origin_id):
        check.str_param(job_origin_id, "job_origin_id")

        if not self.get_job_state(job_origin_id):
            raise DagsterInvariantViolationError(
                "JobState {id} is not present in storage".format(id=job_origin_id)
            )

        with self.connect() as conn:
            conn.execute(
                JobTable.delete().where(  # pylint: disable=no-value-for-parameter
                    JobTable.c.job_origin_id == job_origin_id
                )
            )

    def get_latest_job_tick(self, job_origin_id):
        check.str_param(job_origin_id, "job_origin_id")

        query = (
            db.select([JobTickTable.c.id, JobTickTable.c.tick_body])
            .select_from(JobTickTable)
            .where(JobTickTable.c.job_origin_id == job_origin_id)
            .order_by(JobTickTable.c.timestamp.desc())
            .limit(1)
        )

        rows = self.execute(query)

        if len(rows) == 0:
            return None

        return JobTick(rows[0][0], deserialize_json_to_dagster_namedtuple(rows[0][1]))

    def get_job_ticks(self, job_origin_id):
        check.str_param(job_origin_id, "job_origin_id")

        query = (
            db.select([JobTickTable.c.id, JobTickTable.c.tick_body])
            .select_from(JobTickTable)
            .where(JobTickTable.c.job_origin_id == job_origin_id)
            .order_by(JobTickTable.c.id.desc())
        )

        rows = self.execute(query)
        return list(
            map(lambda r: JobTick(r[0], deserialize_json_to_dagster_namedtuple(r[1])), rows)
        )

    def create_job_tick(self, job_tick_data):
        check.inst_param(job_tick_data, "job_tick_data", JobTickData)

        with self.connect() as conn:
            try:
                tick_insert = JobTickTable.insert().values(  # pylint: disable=no-value-for-parameter
                    job_origin_id=job_tick_data.job_origin_id,
                    status=job_tick_data.status.value,
                    type=job_tick_data.job_type.value,
                    execution_key=job_tick_data.execution_key,
                    timestamp=utc_datetime_from_timestamp(job_tick_data.timestamp),
                    tick_body=serialize_dagster_namedtuple(job_tick_data),
                )
                result = conn.execute(tick_insert)
                tick_id = result.inserted_primary_key[0]
                return JobTick(tick_id, job_tick_data)
            except db.exc.IntegrityError as exc:
                six.raise_from(
                    DagsterInvariantViolationError(
                        "Unable to insert JobTick for job {job_name} in storage".format(
                            job_name=job_tick_data.job_name,
                        )
                    ),
                    exc,
                )

    def update_job_tick(self, tick):
        check.inst_param(tick, "tick", JobTick)

        with self.connect() as conn:
            conn.execute(
                JobTickTable.update()  # pylint: disable=no-value-for-parameter
                .where(JobTickTable.c.id == tick.tick_id)
                .values(
                    status=tick.status.value,
                    type=tick.job_type.value,
                    execution_key=tick.execution_key,
                    timestamp=utc_datetime_from_timestamp(tick.timestamp),
                    tick_body=serialize_dagster_namedtuple(tick.job_tick_data),
                )
            )

        return tick

    def wipe(self):
        """Clears the schedule storage."""
        with self.connect() as conn:
            # https://stackoverflow.com/a/54386260/324449
            conn.execute(ScheduleTable.delete())  # pylint: disable=no-value-for-parameter
            conn.execute(ScheduleTickTable.delete())  # pylint: disable=no-value-for-parameter
            conn.execute(JobTable.delete())  # pylint: disable=no-value-for-parameter
            conn.execute(JobTickTable.delete())  # pylint: disable=no-value-for-parameter
