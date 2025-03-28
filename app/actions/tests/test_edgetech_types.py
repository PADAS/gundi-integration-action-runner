from ast import List
from datetime import datetime, timedelta, timezone

import pytest

from app.actions.edgetech.types import Buoy


@pytest.mark.usefixtures
def test_create_observations(
    get_mock_edgetech_data, get_expected_observations_test_create_observations
):
    """
    Test the create_observations method of the Buoy class.

    - Test using the latest location from previous record when newer record without location exists.
    - Test creating observations from multiple buoys.
    - Test creating observations from both changeRecords and currentState.
    """
    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]

    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations("edgetech_")
        observations.extend(buoy_observations)

    expected_observations = get_expected_observations_test_create_observations
    expected_observations.sort(key=lambda x: x["recorded_at"])
    observations.sort(key=lambda x: x["recorded_at"])
    # Assert
    assert observations == expected_observations

@pytest.mark.usefixtures
def test_create_observations_recored_at_same_as_current_state(get_mock_edgetech_data):
    """
    Test the create_observations method of the Buoy class with a changeRecord with recorded_at
    at the same time as the currentState.
    """

    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]
    last_observation_timestamp = datetime.fromisoformat("2025-03-17T16:43:40").replace(
        tzinfo=timezone.utc
    )
    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations(
            "edgetech_", last_observation_timestamp
        )
        observations.extend(buoy_observations)

    # Assert
    assert observations == []


@pytest.mark.usefixtures
def test_create_observations_with_last_updated_timestamp(
    get_mock_edgetech_data,
    get_expected_observations_test_create_observations_with_last_updated_timestamp,
):
    """
    Test the create_observations method of the Buoy class with a last
    updated timestamp (last_observation_timestamp) provided.

    - Test creating observations from multiple buoys.
    - Test creating observations from both changeRecords and
      currentState.
    - Test creating observations with a last updated timestamp
        - Prevents creating observations with a recorded_at timestamp
          earlier than the last updated timestamp.
    """

    # Arrange
    buoys: List[Buoy] = [Buoy.parse_obj(record) for record in get_mock_edgetech_data]
    last_observation_timestamp = datetime.fromisoformat("2025-03-17T16:43:40").replace(
        tzinfo=timezone.utc
    )

    buoys[12].changeRecords[0].changes[1].newValue = (
        buoys[12].currentState.dateRecovered - timedelta(minutes=2)
    ).isoformat()

    # Act
    observations = []
    for buoy in buoys:
        buoy_observations, _, _ = buoy.create_observations(
            "edgetech_", last_observation_timestamp
        )
        observations.extend(buoy_observations)

    # Assert
    expected_observations = (
        get_expected_observations_test_create_observations_with_last_updated_timestamp
    )

    assert observations == expected_observations
