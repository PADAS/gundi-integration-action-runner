# gundi-integration-action-runner
Template repo for integration in Gundi v2.

## Usage
- Fork this repo
- Implement your own actions in `action/handlers.py`
- Define configurations needed for your actions in `action/config.py`
- Optionally, add the @activity_logger decorator to log common events which you can later see in the portal:
    - Action execution started
    - Action execution finished
    - Error occurred during action execution
- Optionally, use the `log_activity()` method to log custom messages which you can later see in the portal


Example:
```python
from app.services.activity_logger import activity_logger, log_activity
from gundi_core.events import LogLevel


@activity_logger()
async def action_pull_observations(integration, action_config):
    
    # Add your business logic to extract data here...
    
    # Optionally, log a custom messages to be shown in the portal
    await log_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=action_config.data
    )
    # The result will be recorded in the portal if using the activity_logger decorator
    return {"events_extracted": 10}
```
