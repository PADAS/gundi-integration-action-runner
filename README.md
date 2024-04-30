# gundi-integration-action-runner
Template repo for integration in Gundi v2.

## Usage
- Fork this repo
- Implement your own actions in `actions/handlers.py`
- Define configurations needed for your actions in `action/config.py`
- Optionally, add the @activity_logger decorator to log common events which you can later see in the portal:
    - Action execution started
    - Action execution finished
    - Error occurred during action execution
- Optionally, use the `log_activity()` method to log custom messages which you can later see in the portal


Example: 

```python
# actions/configurations.py
from .core import PullActionConfiguration


class PullObservationsConfiguration(PullActionConfiguration):
    lookback_days: int = 10


```

```python
# actions/handlers.py
from app.services.activity_logger import activity_logger, log_activity
from app.services.gundi import send_observations_to_gundi
from gundi_core.events import LogLevel
from .configurations import PullObservationsConfiguration


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfiguration):
    
    # Add your business logic to extract data here...
    
    # Optionally, log a custom messages to be shown in the portal
    await log_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=action_config.dict()
    )
    
    # Normalize the extracted data into a list of observations following to the Gundi schema:
    observations = [
        {
            "source": "collar-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 10
            }
        }
    ]
    
    # Send the extracted data to Gundi
    await send_observations_to_gundi(observations=observations, integration_id=integration.id)

    # The result will be recorded in the portal if using the activity_logger decorator
    return {"observations_extracted": 10}
```
