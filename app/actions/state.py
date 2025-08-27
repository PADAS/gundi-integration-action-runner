from datetime import datetime, timezone, timedelta
import pydantic

def default_last_run():
    '''Default for a new configuration is to pretend the last run was 7 days ago'''
    return datetime.now(tz=timezone.utc) - timedelta(days=7)

class IntegrationState(pydantic.BaseModel):
    last_run: datetime = pydantic.Field(default_factory=default_last_run, alias='last_run')
    error: str = None

    @pydantic.validator("last_run")
    def clean_last_run(cls, v):
        if v is None:
            return default_last_run()
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v