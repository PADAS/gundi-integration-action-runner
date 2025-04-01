from datetime import timedelta
import pytest


@pytest.mark.asyncio
async def test_trap_create_observations_force_create(
    mock_rmwhub_items, mock_er_subjects
):
    # Setup mock gear and er_subject
    mock_gear = mock_rmwhub_items[0]
    mock_gear.deployment_type = "trawl"
    mock_trap = mock_gear.traps[0]
    mock_trap.status = "deployed"

    mock_er_subject = mock_er_subjects[0]
    mock_er_subject["name"] = "rmwhub_" + mock_trap.id
    mock_er_subject["additional"]["devices"] = [
        {
            "device_id": mock_er_subject["name"],
            "label": "a",
            "location": {
                "latitude": mock_trap.latitude,
                "longitude": mock_trap.longitude,
            },
            "last_updated": mock_trap.deploy_datetime_utc,
        }
    ]

    expected_recorded_at = (
        mock_trap.get_latest_update_time() + timedelta(seconds=5)
    ).isoformat()

    observations = await mock_gear.create_observations(mock_er_subject)

    assert len(observations) == 2
    for observation in observations:
        if observation["name"] == "rmwhub_" + mock_trap.id:
            assert observation["recorded_at"] == expected_recorded_at
