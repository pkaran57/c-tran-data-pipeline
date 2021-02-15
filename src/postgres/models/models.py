from sqlalchemy import Column, Integer, ForeignKey, Enum, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_repr import RepresentableBase

from src.postgres.models.ServiceType import ServiceType
from src.postgres.models.TripDirType import TripDirType

Base = declarative_base(cls=RepresentableBase)


class Trip(Base):
    __tablename__ = 'Trip'

    trip_id = Column(Integer, primary_key=True, autoincrement=False)
    route_id = Column(Integer)
    vehicle_id = Column(Integer)
    service_key = Column(Enum(ServiceType, name='service_type'))
    direction = Column(Enum(TripDirType, name='tripdir_type'))


class BreadCrumb(Base):
    __tablename__ = 'BreadCrumb'

    id = Column(Integer, primary_key=True)
    tstamp = Column(TIMESTAMP)
    latitude = Column(Float)
    longitude = Column(Float)
    direction = Column(Integer)
    speed = Column(Float)
    trip_id = Column(Integer, ForeignKey('Trip.trip_id'))
