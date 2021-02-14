from dataclasses import dataclass

from sqlalchemy import Column, Integer, Date, ForeignKey, DateTime, SmallInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_repr import RepresentableBase

Base = declarative_base(cls=RepresentableBase)


@dataclass(init=False, repr=False)
class Vehicle(Base):
    __tablename__ = 'vehicle'
    id = Column(Integer, primary_key=True, autoincrement=False)


@dataclass(init=False, repr=False)
class Trip(Base):
    __tablename__ = 'trip'

    id = Column(Integer, primary_key=True, autoincrement=False)
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'))
    date = Column(Date, nullable=False)


@dataclass(init=False, repr=False)
class Stop(Base):
    __tablename__ = 'stop'

    id = Column(Integer, primary_key=True, autoincrement=False)
    trip_id = Column(Integer, ForeignKey('trip.id'))


class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    stop_id = Column(Integer, ForeignKey('stop.id'))

    time = Column(DateTime, nullable=False)
    meters = Column(Integer, nullable=False)

    velocity = Column(SmallInteger)
    direction = Column(SmallInteger)

    gps_longitude = Column(Float, nullable=False)
    gps_latitude = Column(Float, nullable=False)
    gps_satellites = Column(SmallInteger, nullable=False)
    gps_hdop = Column(Float, nullable=False)

    schedule_deviation = Column(SmallInteger)
