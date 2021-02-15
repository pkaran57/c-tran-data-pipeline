from datetime import date, datetime, timedelta
from pydantic import BaseModel, validator

from src.postgres.models import models


class BreadCrumb(BaseModel):
    event_no_trip: int
    event_no_stop: int
    opd_date: date
    vehicle_id: int
    meters: int
    act_time: int
    velocity: int = None
    direction: int = None
    radio_quality: str = None
    gps_longitude: float
    gps_latitude: float
    gps_satellites: int
    gps_hdop: float
    schedule_deviation: int = None

    @validator('direction')
    def validate_direction(cls, direction):
        if direction:
            if direction < 0 or direction > 359:
                raise ValueError('direction should be between 0 and 359 but is {}'.format(direction))
        return direction

    @validator('gps_latitude')
    def validate_gps_latitude(cls, gps_latitude):
        if gps_latitude < -90 or gps_latitude > 90:
            raise ValueError('gps_latitude should be between -90 and 90 but is {}'.format(gps_latitude))
        return gps_latitude

    @validator('gps_longitude')
    def validate_gps_longitude(cls, gps_longitude):
        if gps_longitude < -180 or gps_longitude > 180:
            raise ValueError('gps_latitude should be between -180 and 180 but is {}'.format(gps_longitude))
        return gps_longitude

    @validator('opd_date', pre=True)
    def parse_opd_date(cls, opd_date):
        if not opd_date:
            raise ValueError('opd_date is falsy')
        return datetime.strptime(opd_date, '%d-%b-%y').date()

    @validator('schedule_deviation', 'velocity', 'direction', pre=True)
    def parse_optional_int(cls, int_str):
        if int_str:
            return int_str
        else:
            return None

    class Config:
        alias_generator = lambda field_key: field_key.upper()

    def get_bread_crumb(self):
        return models.BreadCrumb(tstamp=self.get_timestamp(),
                                 latitude=self.gps_latitude,
                                 longitude=self.gps_longitude,
                                 direction=self.direction,
                                 speed=self.velocity,
                                 trip_id=self.event_no_trip)

    def get_trip(self):
        return models.Trip(trip_id=self.event_no_trip,
                           vehicle_id=self.vehicle_id)

    def get_timestamp(self):
        time = datetime(self.opd_date.year, self.opd_date.month, self.opd_date.day) + timedelta(0, self.act_time)
        return time
