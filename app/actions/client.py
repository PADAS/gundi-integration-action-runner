import logging
import httpx
import pydantic
import xmltodict

from datetime import datetime
from app.actions.configurations import (
    AuthenticateConfig,
    PullObservationsConfig
)
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from xml.parsers.expat import ExpatError
from typing import List, Optional


logger = logging.getLogger(__name__)


# Pydantic models
class DataResponse(pydantic.BaseModel):
    ats_serial_num: str = pydantic.Field(..., alias="AtsSerialNum")
    longitude: float = pydantic.Field(None, alias='Longitude', ge=-180.0, le=360.0)
    latitude: float = pydantic.Field(None, alias='Latitude', ge=-90.0, le=90.0)
    date_year_and_julian: datetime = pydantic.Field(..., alias="DateYearAndJulian")
    num_sats: Optional[str] = pydantic.Field(None, alias='NumSats')
    hdop: Optional[str] = pydantic.Field(None, alias='Hdop')
    fix_time: Optional[str] = pydantic.Field(None, alias='FixTime')
    dimension: Optional[str] = pydantic.Field(None, alias='Dimension')
    activity: Optional[str] = pydantic.Field(None, alias='Activity')
    temperature: Optional[str] = pydantic.Field(None, alias='Temperature')
    mortality: Optional[bool] = pydantic.Field(None, alias='Mortality')
    low_batt_voltage: Optional[bool] = pydantic.Field(None, alias='LowBattVoltage')

    class Config:
        allow_population_by_field_name = True

    """
    @validator('DateYearAndJulian')
    def parse_datetime(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v
    """


class TransmissionsResponse(pydantic.BaseModel):
    date_sent: datetime = pydantic.Field(..., alias="DateSent")
    collar_serial_num: str = pydantic.Field(..., alias="CollarSerialNum")
    number_fixes: Optional[int] = pydantic.Field(None, alias='NumberFixes')
    batt_voltage: Optional[float] = pydantic.Field(None, alias='BattVoltage')
    mortality: Optional[str] = pydantic.Field(None, alias='Mortality')
    break_off: Optional[str] = pydantic.Field(None, alias='BreakOff')
    sat_errors: Optional[str] = pydantic.Field(None, alias='SatErrors')
    year_base: Optional[str] = pydantic.Field(None, alias='YearBase')
    day_base: Optional[str] = pydantic.Field(None, alias='DayBase')
    gmt_offset: Optional[int] = pydantic.Field(None, alias='GmtOffset')
    low_batt_voltage: Optional[bool] = pydantic.Field(None, alias='LowBattVoltage')

    class Config:
        allow_population_by_field_name = True

    """
    @validator('DateSent')
    def parse_datetime(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v
    """


class PullObservationsDataResponse(pydantic.BaseModel):
    vehicles: List[DataResponse]

    @pydantic.validator("vehicles", pre=True)
    def validate_vehicles(cls, val):
        if isinstance(val, list):
            return val
        # val is not a valid list, return an empty list instead
        return []


class PullObservationsTransmissionsResponse(pydantic.BaseModel):
    transmissions: List[TransmissionsResponse]

    @pydantic.validator("transmissions", pre=True)
    def validate_transmissions(cls, val):
        if isinstance(val, list):
            return val
        # val is not a valid list, return an empty list instead
        return []


class PullObservationsBadXMLException(Exception):
    def __init__(self, error: Exception, message: str, status_code=422):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)


def closest_transmission(transmissions, test_date):
    sorted_list = sorted([t.DateSent for t in transmissions])
    previous_date = sorted_list[-1]
    for date in sorted_list:
        if date >= test_date:
            if abs((date - test_date).days) < abs((previous_date - test_date).days):
                return [t for t in transmissions if t.DateSent == date][0]
            else:
                return [t for t in transmissions if t.DateSent == previous_date][0]
        previous_date = date
    return [t for t in transmissions if t.DateSent == sorted_list[-1]][0]


async def get_data_endpoint_response(config, auth):
    endpoint = config.data_endpoint
    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.get(endpoint, auth=(auth.username, auth.password.get_secret_value()))
        response.raise_for_status()
        try:
            parsed_xml = xmltodict.parse(response.text)
        except (xmltodict.ParsingInterrupted, ExpatError) as e:
            msg = "Error while parsing XML from 'data' endpoint"
            logger.exception(
                msg,
                extra={
                    "attention_needed": True,
                    "endpoint": endpoint,
                    "username": auth.username
                }
            )
            raise PullObservationsBadXMLException(message=msg, error=e)
        else:
            try:
                data_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
                data = data_xml_tag.get("NewDataSet", {})
            except KeyError as e:
                msg = "Error while parsing 'data' response from XML"
                logger.exception(
                    msg,
                    extra={
                        "attention_needed": True,
                        "endpoint": endpoint,
                        "username": auth.username
                    }
                )
                raise PullObservationsBadXMLException(message=msg, error=e)
            else:
                if data:
                    try:
                        parsed_response = PullObservationsDataResponse.parse_obj(
                            {"vehicles": data.get("Table", [])}
                        )
                    except pydantic.ValidationError as e:
                        msg = "Error while parsing 'PullObservationsDataResponse' response from XML (data)"
                        logger.exception(
                            msg,
                            extra={
                                "attention_needed": True,
                                "endpoint": endpoint,
                                "username": auth.username
                            }
                        )
                        raise PullObservationsBadXMLException(message=msg, error=e)
                    else:
                        response = parsed_response.vehicles
                else:
                    response = []

    return response


async def get_transmissions_endpoint_response(config, auth):
    endpoint = config.transmissions_endpoint
    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.get(endpoint, auth=(auth.username, auth.password.get_secret_value()))
        response.raise_for_status()
        try:
            parsed_xml = xmltodict.parse(response.text)
        except (xmltodict.ParsingInterrupted, ExpatError) as e:
            msg = "Error while parsing XML from 'transmissions' endpoint"
            logger.exception(
                msg,
                extra={
                    "attention_needed": True,
                    "endpoint": endpoint,
                    "username": auth.username
                }
            )
            raise PullObservationsBadXMLException(message=msg, error=e)
        else:
            try:
                transmissions_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
                transmissions = transmissions_xml_tag.get("NewDataSet", {})
            except KeyError as e:
                msg = "Error while parsing 'transmissions' response from XML"
                logger.exception(
                    msg,
                    extra={
                        "attention_needed": True,
                        "endpoint": endpoint,
                        "username": auth.username
                    }
                )
                raise PullObservationsBadXMLException(message=msg, error=e)
            else:
                if transmissions:
                    try:
                        parsed_response = PullObservationsTransmissionsResponse.parse_obj(
                            {"transmissions": transmissions.get("Table", [])}
                        )
                    except pydantic.ValidationError as e:
                        msg = "Error while parsing 'PullObservationsTransmissionsResponse' response from XML (data)"
                        logger.exception(
                            msg,
                            extra={
                                "attention_needed": True,
                                "endpoint": endpoint,
                                "username": auth.username
                            }
                        )
                        raise PullObservationsBadXMLException(message=msg, error=e)
                    else:
                        response = parsed_response.transmissions
                else:
                    response = []

    return response
