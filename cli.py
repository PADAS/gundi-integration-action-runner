import click

from datetime import datetime, timedelta, timezone
import asyncio
from app.actions.gfwclient import DataAPI, Geostore, GeostoreView,CreatedGeostore
from app.actions import utils
from shapely.geometry import GeometryCollection, shape, mapping
from app.services.state import IntegrationStateManager


@click.group(help="A group of commands for getting data from a SMART Connect Server")
def cli():
    pass

common_options = [
    click.option('--username', help='username/email used to log into Global Forest Watch dashboard', required=True),
    click.option('--password', help='password', required=True),
]

def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func
    return _add_options

@cli.command(help="Fetch AOI Information for the provided URL")
@click.argument('url', type=str)
@add_options(common_options)
def aoi_info(url, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(aoi_data.json(indent=2))
    asyncio.run(fn())


@cli.command(help="Fetch Geostore Information for the provided URL")
@click.argument('url', type=str)
@add_options(common_options)
def geostore_info(url, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
        geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)
        print(geostore_data.json(indent=2))
    asyncio.run(fn())


@cli.command(help="Get NASA Viirs Fire Alerts for the provided URL")
@click.argument('url', type=str)
@click.option('--days', type=int, required=False, default=7)
@add_options(common_options)
def nasa_viirs_fire_alerts(url, username, password, days):
    client = DataAPI(username=username, password=password)

    async def fn():
        aoi_id = await client.aoi_from_url(url)
        print(f'AOI ID: {aoi_id}')
        aoi_data = await client.get_aoi(aoi_id=aoi_id)
        print(f'GEOSTORE ID: {aoi_data.attributes.geostore}')
        geostore_data = await client.get_geostore(geostore_id=aoi_data.attributes.geostore)


        geometry_collection = GeometryCollection(
            [
                shape(feature["geometry"]).buffer(0)
                for feature in geostore_data.attributes.geojson["features"]
            ]
        )
        for partition in utils.generate_geometry_fragments(geometry_collection=geometry_collection):

            geostore:CreatedGeostore = await client.create_geostore(geometry=mapping(partition))

            fire_alerts = await client.get_nasa_viirs_fire_alerts(
                geostore_id=geostore.gfw_geostore_id,
                date_range=(datetime.now(tz=timezone.utc) - timedelta(days=days), datetime.now(tz=timezone.utc)),
                semaphore=asyncio.Semaphore(5)    
            )
            print(fire_alerts)
    asyncio.run(fn())

@cli.command(help="List Datasets")
def datasets():
    client = DataAPI()

    async def fn():
        datasets = await client.get_datasets()
        print(datasets.json(indent=2))
    asyncio.run(fn())

@cli.command(help="Dataset Metadata")
@click.argument('dataset', type=str)
@add_options(common_options)
def dataset_metadata(dataset, username, password):
    client = DataAPI(username=username, password=password)

    async def fn():
        metadata = await client.get_dataset_metadata(dataset=dataset)
        print(metadata.json(indent=2))
    asyncio.run(fn())
    
@cli.command(help="Get Datasets")
@click.option('--search', type=str, required=False)
def get_datasets(search=None):
    client = DataAPI()

    async def fn():
        datasets = await client.get_datasets()
        return datasets
    val = asyncio.run(fn())

    if search:
        search = search.lower()
        val = [d for d in val if search in d.dataset.lower()]

    for d in val:
        print(d.dataset)


@cli.command(help="Get Dataset Fields")
@click.argument('dataset', type=str, required=True)
@click.option('--version', type=str, required=False, default="latest")
def get_dataset_fields(dataset, version):
    client = DataAPI()

    async def fn():
        f = await client.get_dataset_fields(dataset=dataset, version=version)
        return f
    val = asyncio.run(fn())
    for v in val:
        print(v.json())


if __name__ == '__main__':
    cli()

