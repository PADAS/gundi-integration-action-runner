from functools import wraps
from pydantic import BaseModel
from pydantic.fields import Field
from pydantic.class_validators import validator
from typing import Any, Dict, Optional, Union, List, Annotated
from gundi_core.commands import RunIntegrationAction
from app import settings
from .activity_logger import publish_event


async def trigger_action(integration_id: str, action_id: str, config=None):
    """
    Publishes a command message in the actions topic to trigger an action.
    Use this function to trigger other actions from the integration.
    :param integration_id: uuid of the integration
    :param action_id: slug id of the action
    :param config: configuration model
    :return:
    """
    run_action_command = RunIntegrationAction(
        integration_id=integration_id,
        action_id=action_id,
        config_overrides=config.dict() if config else None
    )
    if settings.TRIGGER_ACTIONS_ALWAYS_SYNC:  # For testing or local development
        from .action_runner import execute_action
        return await execute_action(
            integration_id=integration_id,
            action_id=action_id,
            config_overrides=config.dict() if config else None
        )
    else:
        if not settings.INTEGRATION_COMMANDS_TOPIC:
            error_msg = "Please set INTEGRATION_COMMANDS_TOPIC in the environment to trigger actions from the integration."
            raise ValueError(error_msg)
        return await publish_event(run_action_command, settings.INTEGRATION_COMMANDS_TOPIC)


class CrontabSchedule(BaseModel):
    minute: str = Field(
        "*",
        regex=r"^(\*|([0-5]?\d)(,([0-5]?\d))*|([0-5]?\d-[0-5]?\d)(/\d+)?|\*(/\d+)?)$"
    )
    hour: str = Field(
        "*",
        regex=r"^(\*|([01]?\d|2[0-3])(,([01]?\d|2[0-3]))*|([01]?\d|2[0-3]-[01]?\d|2[0-3])(/\d+)?|\*(/\d+)?)$"
    )
    day_of_week: str = Field(
        "*",
        regex=r"^(\*|[0-6](,[0-6])*|([0-6]-[0-6])(/\d+)?|\*(/\d+)?)$"
    )
    day_of_month: str = Field(
        "*",
        regex=r"^(\*|([1-9]|[12]\d|3[01])(,([1-9]|[12]\d|3[01]))*|([1-9]|[12]\d|3[01]-[1-9]|[12]\d|3[01])(/\d+)?|\*(/\d+)?)$"
    )
    month_of_year: str = Field(
        "*",
        regex=r"^(\*|([1-9]|1[0-2])(,([1-9]|1[0-2]))*|([1-9]|1[0-2]-[1-9]|1[0-2])(/\d+)?|\*(/\d+)?)$"
    )
    tz_offset: int = Field(
        0,
        description="Timezone offset from UTC, e.g., 0 for UTC, -5 for UTC-5, +2 for UTC+2"
    )

    @validator("tz_offset")
    def validate_timezone(cls, value):
        """Validate that timezone is an integer between -12 and +14."""
        if not (-12 <= value <= 14):
            raise ValueError("Timezone offset must be between -12 and +14.")
        return value

    @validator("minute", "hour", "day_of_week", "day_of_month", "month_of_year")
    def validate_crontab_field(cls, value, field):
        if not value:
            raise ValueError(f"{field.name} cannot be empty.")
        return value

    # build from crontab string
    @classmethod
    def parse_obj_from_crontab(cls, crontab: str):
        parts = crontab.split()
        if len(parts) == 6:
            minute, hour, day_of_month, month_of_year, day_of_week, tz_offset = parts
        elif len(parts) == 5:
            minute, hour, day_of_month, month_of_year, day_of_week = parts
            tz_offset = 0
        else:
            raise ValueError("Invalid crontab format. Must have 5 or 6 fields.")

        return cls(
            minute=minute,
            hour=hour,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
            day_of_week=day_of_week,
            tz_offset=int(tz_offset)
        )


# Defines when a periodic action runs. Can receive a CrontabSchedule object or a string as an argument
def crontab_schedule(crontab: Union[CrontabSchedule, str]):
    def decorator(func):
        if isinstance(crontab, str):
            schedule = CrontabSchedule.parse_obj_from_crontab(crontab)
        else:
            schedule = crontab
        setattr(func, "crontab_schedule", schedule)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        return wrapper
    return decorator

