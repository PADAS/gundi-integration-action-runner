import pytest

from app.actions.buoy import ObservationSubject
from app.actions.edgetech import EdgeTechProcessor
from app.actions.edgetech.types import Buoy


@pytest.mark.asyncio
async def test_edgetech_processor_updated_buoys(
    mocker, get_mock_edgetech_data, get_er_subjects_updated_data
):
    """
    Test if the EdgeTechProcessor returns the correct updated buoys
    when the buoys are already in the newest state in the ER site.

    - The EdgeTechProcessor should return an empty list of observations
    - The EdgeTechProcessor should return an empty set of inserts_buoys
    - The EdgeTechProcessor should return an empty set of update_buoys
    """
    # Arrange
    buoys = [Buoy.parse_obj(buoy) for buoy in get_mock_edgetech_data]
    subjects = [
        ObservationSubject.parse_obj(subject)
        for subject in get_er_subjects_updated_data
    ]

    mocker.patch("app.actions.buoy.BuoyClient.get_er_subjects", return_value=subjects)
    # Act
    processor = EdgeTechProcessor(buoys, "er_token", "er_token")
    observations, inserts_buoys, update_buoys = await processor.process()
    assert observations == []
    assert inserts_buoys == set()
    assert update_buoys == set()


@pytest.mark.asyncio
async def test_edgetech_processor_inserting_buoys(
    mocker,
    get_mock_edgetech_data,
    get_expected_observations_test_edgetech_processor_inserting_buoys,
):
    """ "
    Test if the EdgeTechProcessor returns the correct observations, inserts_buoys and update_buoys
    when the buoys are not in the ER site.

    - The EdgeTechProcessor should return the correct observations
    - The EdgeTechProcessor should return the correct set of inserts_buoys
    - The EdgeTechProcessor should return an empty set of update_buoys
    """

    # Arrange
    buoys = [Buoy.parse_obj(buoy) for buoy in get_mock_edgetech_data]
    subjects = []

    mocker.patch("app.actions.buoy.BuoyClient.get_er_subjects", return_value=subjects)

    # Act
    processor = EdgeTechProcessor(buoys, "er_token", "er_token")
    observations, inserts_buoys, update_buoys = await processor.process()

    expected_observations = (
        get_expected_observations_test_edgetech_processor_inserting_buoys
    )
    expected_inserts_buoys = {buoy.serialNumber for buoy in buoys}

    # Assert
    observations.sort(key=lambda x: x["recorded_at"])
    expected_observations.sort(key=lambda x: x["recorded_at"])
    assert observations == expected_observations
    assert inserts_buoys == expected_inserts_buoys
    assert update_buoys == set()
