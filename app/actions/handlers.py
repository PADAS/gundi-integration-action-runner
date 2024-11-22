import datetime
import httpx
import logging
import stamina
import app.actions.client as client
import app.services.gundi as gundi_tools
from app.services.activity_logger import activity_logger
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


async def filter_and_transform(vehicles, transmissions, integration_id, action_id):
    transformed_data = []
    main_data = ["ats_serial_num", "date_year_and_julian", "latitude", "longitude"]

    vehicles_to_skip = []  # FOR NOW, THESE VEHICLES WILL BE SKIPPED BECAUSE NO GMT OFFSET SET

    # Get GMT offset per serial num
    serial_nums = set([v.ats_serial_num for v in vehicles])
    gmt_offsets = {}
    for serial_num in serial_nums:
        try:
            gmt_offset = next(iter(set([t.gmt_offset for t in transmissions if t.collar_serial_num == serial_num])))
            # check for invalid offsets
            if gmt_offset is None:
                gmt_offsets[serial_num] = 0
            elif abs(gmt_offset) <= 24:
                gmt_offsets[serial_num] = gmt_offset
            else:
                vehicles_to_skip.append(serial_num)
                message = f"GMT offset invalid for device '{serial_num}' value '{gmt_offset}'"
                logger.warning(
                    message,
                    extra={
                        'needs_attention': True,
                        'integration_id': integration_id,
                        'action_id': action_id
                    }
                )
        except StopIteration:
            # If no offset found, a 0 offset will be set (new collar)
            gmt_offsets[serial_num] = 0

    for vehicle in vehicles:
        if vehicle.ats_serial_num not in vehicles_to_skip:
            # Get GmtOffset for this device
            gmt_offset_value = gmt_offsets.get(vehicle.ats_serial_num)
            time_delta = datetime.timedelta(hours=gmt_offset_value)
            timezone_object = datetime.timezone(time_delta)

            date_year_and_julian_with_tz = vehicle.date_year_and_julian.replace(tzinfo=timezone_object)

            vehicle.date_year_and_julian = date_year_and_julian_with_tz

            # Get current state for the device
            current_state = await state_manager.get_state(
                integration_id,
                action_id,
                vehicle.ats_serial_num
            )

            if current_state:
                # Compare current state with new data
                latest_device_timestamp = datetime.datetime.strptime(
                    current_state.get("latest_device_timestamp"),
                    '%Y-%m-%d %H:%M:%S%z'
                )

                if vehicle.date_year_and_julian <= latest_device_timestamp:
                    # Data is not new, not transform
                    logger.info(
                        f"Excluding device ID '{vehicle.ats_serial_num}' obs '{vehicle.date_year_and_julian}'"
                    )
                    continue

            data = {
                "source": vehicle.ats_serial_num,
                "source_name": vehicle.ats_serial_num,
                'type': 'tracking-device',
                "recorded_at": vehicle.date_year_and_julian,
                "location": {
                    "lat": vehicle.latitude,
                    "lon": vehicle.longitude
                },
                "additional": {
                    key: value for key, value in vehicle.dict().items()
                    if key not in main_data and value is not None
                }
            }
            transformed_data.append(data)

    return transformed_data


@activity_logger()
async def action_pull_observations(integration, action_config: client.PullObservationsConfig):
    logger.info(
        f"Executing pull_observations action with integration {integration} and action_config {action_config}..."
    )
    observations_extracted = []
    try:
        async for attempt in stamina.retry_context(
                on=httpx.HTTPError,
                attempts=3,
                wait_initial=datetime.timedelta(seconds=10),
                wait_max=datetime.timedelta(seconds=10),
        ):
            with attempt:
                vehicles = await client.get_data_endpoint_response(
                    config=client.get_pull_config(integration),
                    auth=client.get_auth_config(integration)
                )

                if vehicles:
                    transmissions = await client.get_transmissions_endpoint_response(
                        config=client.get_pull_config(integration),
                        auth=client.get_auth_config(integration)
                    )
                else:
                    logger.warning(f"No observations were pulled.")
                    return {"message": "No observations pulled"}

                if not transmissions:
                    logger.warning(f"No transmissions were pulled.")
                    return {"message": "No transmissions pulled"}

        logger.info(f"Observations pulled with success.")

        transformed_data = await filter_and_transform(
            vehicles,
            transmissions,
            str(integration.id),
            "pull_observations"
        )

        if transformed_data:
            # Send transformed data to Sensors API V2
            def generate_batches(iterable, n=action_config.observations_per_request):
                for i in range(0, len(iterable), n):
                    yield iterable[i: i + n]

            for i, batch in enumerate(generate_batches(transformed_data)):
                async for attempt in stamina.retry_context(
                        on=httpx.HTTPError,
                        attempts=3,
                        wait_initial=datetime.timedelta(seconds=10),
                        wait_max=datetime.timedelta(seconds=10),
                ):
                    with attempt:
                        try:
                            logger.info(
                                f'Sending observations batch #{i}: {len(batch)} observations'
                            )
                            response = await gundi_tools.send_observations_to_gundi(
                                observations=batch,
                                integration_id=integration.id
                            )
                        except httpx.HTTPError as e:
                            msg = f'Sensors API returned error for integration_id: {str(integration.id)}. Exception: {e}'
                            logger.exception(
                                msg,
                                extra={
                                    'needs_attention': True,
                                    'integration_id': str(integration.id),
                                    'action_id': "pull_observations"
                                }
                            )
                            raise e
                        else:
                            if response:
                                # Update states
                                for vehicle in batch:
                                    state = {
                                        "latest_device_timestamp": vehicle.get("recorded_at"),
                                        "mortality": vehicle["additional"].get("mortality")
                                    }
                                    await state_manager.set_state(
                                        str(integration.id),
                                        "pull_observations",
                                        state,
                                        vehicle.get("source"),
                                    )
                                    observations_extracted.append(response)
        return {'observations_extracted': observations_extracted or []}

    except httpx.HTTPError as e:
        message = f"pull_observations action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e