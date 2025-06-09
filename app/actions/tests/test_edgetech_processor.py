import pytest

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from freezegun import freeze_time

from app.actions import edgetech
from app.actions.buoy import ObservationSubject
from app.actions.edgetech.processor import EdgeTechProcessor

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.usefixtures()
async def test_process_new_edgetech_trawl(mocker, a_new_edgetech_trawl_record):
    """Test that the default filters include start_date and end_date."""
    # Arrange
    data = [a_new_edgetech_trawl_record]
    processor = EdgeTechProcessor(data=data, er_token="token", er_url="url")
    mock_er_client = mocker.MagicMock()
    mock_er_client.get_er_subjects = AsyncMock(return_value=[])
    processor._er_client = mock_er_client

    # Act
    observations = await processor.process()

    # Assert
    logger.info(f"Processed observations: {observations}")
    expected_observvations = [
        {
            "name": "edgetech_8899CEDAAA_A",
            "source": "edgetech_8899CEDAAA_A",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2025-05-25T17:53:19+00:00",
            "location": {"lat": 44.358265, "lon": -68.16757},
            "additional": {
                "subject_name": "edgetech_8899CEDAAA_A",
                "edgetech_serial_number": "8899CEDAAA",
                "display_id": "12daa93d83a5",
                "subject_is_active": True,
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 44.358265, "longitude": -68.16757},
                        "device_id": "edgetech_8899CEDAAA_A",
                        "last_updated": "2025-05-25T17:53:19+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 44.3591792, "longitude": -68.167191},
                        "device_id": "edgetech_8899CEDAAA_B",
                        "last_updated": "2025-05-25T17:53:19+00:00",
                    },
                ],
            },
        },
        {
            "name": "edgetech_8899CEDAAA_B",
            "source": "edgetech_8899CEDAAA_B",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "is_active": True,
            "recorded_at": "2025-05-25T17:53:19+00:00",
            "location": {"lat": 44.3591792, "lon": -68.167191},
            "additional": {
                "subject_name": "edgetech_8899CEDAAA_B",
                "edgetech_serial_number": "8899CEDAAA",
                "display_id": "12daa93d83a5",
                "subject_is_active": True,
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 44.358265, "longitude": -68.16757},
                        "device_id": "edgetech_8899CEDAAA_A",
                        "last_updated": "2025-05-25T17:53:19+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 44.3591792, "longitude": -68.167191},
                        "device_id": "edgetech_8899CEDAAA_B",
                        "last_updated": "2025-05-25T17:53:19+00:00",
                    },
                ],
            },
        },
    ]
    assert observations == expected_observvations


@pytest.mark.asyncio
@pytest.mark.usefixtures()
@freeze_time("2025-05-25T17:53:19+00:00")
async def test_process_deployed_in_er_missing_in_edgetech(
    mocker, a_deployed_earthranger_subject
):
    """Test that the processor handles an empty data list."""
    # Arrange
    data = []
    processor = EdgeTechProcessor(data=data, er_token="token", er_url="url")
    mock_er_client = mocker.MagicMock()
    mock_er_client.get_er_subjects = AsyncMock(
        return_value=[
            ObservationSubject.parse_obj(a_deployed_earthranger_subject),
        ]
    )
    processor._er_client = mock_er_client

    # Act
    observations = await processor.process()

    # Assert
    logger.info(f"Processed observations: {observations}")
    expected_observations = [
        {
            "name": "edgetech_88CE999CAA_A",
            "source": "edgetech_88CE999CAA_A",
            "type": "unassigned",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2025-05-25T17:53:19+00:00",
            "location": {"lat": 37.997533, "lon": -122.9417194},
            "additional": {
                "subject_name": "edgetech_88CE999CAA_A",
                "edgetech_serial_number": "88CE999CAA",
                "display_id": "aa1b1aefc7d0",
                "subject_is_active": True,
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 37.4802635, "longitude": -122.5286185},
                        "device_id": "edgetech_88CE999CAA_A",
                        "last_updated": "2023-10-13T18:42:33+00:00",
                    }
                ],
            },
        }
    ]
    assert observations == expected_observations
