from itertools import chain

from dagster_graphql.schema.logs.log_level import GrapheneLogLevel
from dagster_graphql.schema.util import HasContext

import dagster._check as check
from dagster._core.definitions.instigation_logger import get_instigation_log_records
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.host_representation import InstigatorSelector
from dagster._core.log_manager import DAGSTER_META_KEY
from dagster._core.scheduler.instigation import InstigatorStatus

from .utils import capture_error


@capture_error
def get_unloadable_instigator_states_or_error(graphene_info: HasContext, instigator_type=None):
    from ..schema.instigation import GrapheneInstigationState, GrapheneInstigationStates

    check.opt_inst_param(instigator_type, "instigator_type", InstigatorType)
    instigator_states = graphene_info.context.instance.all_instigator_state(
        instigator_type=instigator_type
    )
    external_instigators = [
        instigator
        for repository_location in graphene_info.context.repository_locations
        for repository in repository_location.get_repositories().values()
        for instigator in chain(
            repository.get_external_schedules(), repository.get_external_sensors()
        )
    ]

    instigator_selector_ids = {
        instigator.selector_id  # type: ignore # mypy getting confused by chain
        for instigator in external_instigators
    }
    unloadable_states = [
        instigator_state
        for instigator_state in instigator_states
        if instigator_state.selector_id not in instigator_selector_ids
        and instigator_state.status == InstigatorStatus.RUNNING
    ]

    return GrapheneInstigationStates(
        results=[
            GrapheneInstigationState(instigator_state=instigator_state)
            for instigator_state in unloadable_states
        ]
    )


@capture_error
def get_instigator_state_or_error(graphene_info, selector):
    from ..schema.instigation import GrapheneInstigationState, GrapheneInstigationStateNotFoundError

    check.inst_param(selector, "selector", InstigatorSelector)
    location = graphene_info.context.get_repository_location(selector.location_name)
    repository = location.get_repository(selector.repository_name)

    if repository.has_external_sensor(selector.name):
        external_sensor = repository.get_external_sensor(selector.name)
        stored_state = graphene_info.context.instance.get_instigator_state(
            external_sensor.get_external_origin_id(),
            external_sensor.selector_id,
        )
        current_state = external_sensor.get_current_instigator_state(stored_state)
    elif repository.has_external_schedule(selector.name):
        external_schedule = repository.get_external_schedule(selector.name)
        stored_state = graphene_info.context.instance.get_instigator_state(
            external_schedule.get_external_origin_id(),
            external_schedule.selector_id,
        )
        current_state = external_schedule.get_current_instigator_state(stored_state)
    else:
        return GrapheneInstigationStateNotFoundError(selector.name)

    return GrapheneInstigationState(current_state)


def get_tick_log_events(graphene_info, tick):
    from ..schema.instigation import GrapheneInstigationEvent, GrapheneInstigationEventConnection

    if not tick.log_key:
        return GrapheneInstigationEventConnection(events=[], cursor="", hasMore=False)

    records = get_instigation_log_records(graphene_info.context.instance, tick.log_key)
    return GrapheneInstigationEventConnection(
        events=[
            GrapheneInstigationEvent(
                message=record_dict[DAGSTER_META_KEY]["orig_message"],
                level=GrapheneLogLevel.from_level(record_dict["levelno"]),
                timestamp=int(record_dict["created"] * 1000),
            )
            for record_dict in records
        ],
        cursor=None,
        hasMore=False,
    )
