import asyncio
import click
from app.services.action_runner import _portal
from app.services.self_registration import register_integration_in_gundi


@click.command()
@click.option('--slug', default=None, help='Slug ID for the integration type')
@click.option('--service-url', default=None, help='Service URL used to trigger actions or receive webhooks')
def register_integration(slug, service_url):
    asyncio.run(register_integration_in_gundi(gundi_client=_portal, type_slug=slug, service_url=service_url))


# Main
if __name__ == "__main__":
    register_integration()
