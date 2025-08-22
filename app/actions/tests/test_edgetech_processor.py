import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from freezegun import freeze_time

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
    mock_er_client.get_er_gears = AsyncMock(return_value=[])
    processor._er_client = mock_er_client

    # Act
    observations = await processor.process()

    # Assert
    logger.info(f"Processed observations: {observations}")
    assert len(observations) == 2

    assert observations[0]["subject_name"] == observations[1]["subject_name"]

    assert observations[0]["subject_type"] == "ropeless_buoy_gearset"
    assert observations[0]["recorded_at"] == "2025-05-25T17:53:19+00:00"
    assert observations[0]["source_type"] == "ropeless_buoy"
    assert observations[0]["manufacturer_id"].startswith("8899CEDAAA")
    assert observations[0]["is_active"] is True
    assert observations[0]["location"] == {"lat": 44.358265, "lon": -68.16757}
    assert observations[0]["additional"]["event_type"] == "gear_deployed"

    assert observations[1]["subject_type"] == "ropeless_buoy_gearset"
    assert observations[1]["recorded_at"] == "2025-05-25T17:53:19+00:00"
    assert observations[1]["source_type"] == "ropeless_buoy"
    assert observations[1]["manufacturer_id"].startswith("8899CEDAAA")
    assert observations[1]["is_active"] is True
    assert observations[1]["location"] == {"lat": 44.3591792, "lon": -68.167191}
    assert observations[1]["additional"]["event_type"] == "gear_deployed"


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
    mock_er_client.get_er_gears = AsyncMock(return_value=[])
    processor._er_client = mock_er_client

    # Act
    observations = await processor.process()

    # Assert
    expected_observations = []
    assert observations == expected_observations
