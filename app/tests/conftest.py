import pytest

@pytest.fixture
def a_good_history_result():
    return {
                "response": "success", 
                "data": [
                    {
                        "device_timezone": -8, 
                        "unit_id": "unit1", 
                        "fixtime": "01/30/2008 06:55:00 AM",
                        "alerts": ["alert1"],
                        "location": "",
                        "speed": 1, 
                        "course": 1,
                        "longitude": 35.5,
                        "latitude": 1.4,
                        "reg_no": "REGNO1",
                        "driver": "Driver Dan"
                    }
                ]
            }

@pytest.fixture
def a_bad_history_result():
    return {
                "response": "success", 
                "data": [
                    {
                        "device_timezone": -8, 
                        "unit_id": "unit1", 
                        "fixtime": "13/30/2008 06:55:00 AM",
                        "alerts": ["alert1"],
                        "location": "",
                        "speed": 1, 
                        "course": 1,
                        "longitude": 35.5,
                        "latitude": 1.4,
                        "reg_no": "REGNO1",
                        "driver": "Driver Dan"
                    }
                ]
            }