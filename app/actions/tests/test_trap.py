import pytest
from datetime import datetime, timedelta, timezone


@pytest.mark.asyncio
async def test_trap_convert_to_utc(mock_rmwhub_items):
    """
    Test trap.convert_to_utc
    """

    # Setup mock trap
    mock_trap = mock_rmwhub_items[0].traps[0]

    datetime_obj = datetime.now(timezone.utc)
    datetime_with_seconds_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    parsed_datetime = mock_trap.convert_to_utc(datetime_with_seconds_str)
    datetime_without_fractional = datetime_obj.replace(microsecond=0)

    assert parsed_datetime
    assert parsed_datetime == datetime_without_fractional
    assert parsed_datetime.strftime("%Y-%m-%d %H:%M:%S") == datetime_with_seconds_str

    # Test with datetime with fractional seconds
    datetime_with_fractional_seconds_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    parsed_datetime_with_fractional_seconds = mock_trap.convert_to_utc(
        datetime_with_fractional_seconds_str
    )

    assert parsed_datetime_with_fractional_seconds
    assert parsed_datetime_with_fractional_seconds == datetime_obj
    assert (
        parsed_datetime_with_fractional_seconds.strftime("%Y-%m-%d %H:%M:%S.%f")
        == datetime_with_fractional_seconds_str
    )


@pytest.mark.asyncio
async def test_trap_shift_update_time(mock_rmwhub_items):
    # Setup mock trap
    mock_trap = mock_rmwhub_items[0].traps[0]
    mock_trap.status = "deployed"

    expected_deployment_time = (
        mock_trap.get_latest_update_time() + timedelta(seconds=5)
    ).isoformat()

    mock_trap.shift_update_time()

    assert mock_trap.deploy_datetime_utc == expected_deployment_time
