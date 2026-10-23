import asyncio
import click
import pydantic

from app.services.action_runner import _portal
from app.services.self_registration import register_integration_in_gundi
from app.services.action_scheduler import CrontabSchedule


@click.command()
@click.option('--slug', default=None, help='Slug ID for the integration type')
@click.option('--name', default=None, help='Display name for the integration type (defaults to a name derived from the slug)')
@click.option('--description', default=None, help='Description shown in the Gundi portal (defaults to "Default type for integrations with <name>")')
@click.option('--service-url', default=None, help='Service URL used to trigger actions or receive webhooks')
@click.option(
    '--schedule',
    multiple=True,
    help='Schedules in the format "action_id:crontab schedule" (e.g., "pull_events:0 */4 * * *")'
)
def register_integration(slug, name, description, service_url, schedule):
    schedules = {}
    for item in schedule:
        try:
            action_id, cron_schedule = item.split(":", 1)
            schedules[action_id.strip()] = CrontabSchedule.parse_obj_from_crontab(cron_schedule.strip())
        except (pydantic.ValidationError, ValueError) as e:
            raise click.BadParameter(
                f"Invalid schedule format: {item}.\n Expected format is 'action_id:MIN HOUR DOM MON DOW [TZ]'. e.g., 'pull_events:0 */4 * * * -5'. \n {e}"
            )
    asyncio.run(
        register_integration_in_gundi(
            gundi_client=_portal,
            type_slug=slug,
            type_name=name,
            type_description=description,
            service_url=service_url,
            action_schedules=schedules
        )
    )


# Main
if __name__ == "__main__":
    register_integration()
