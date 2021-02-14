import logging
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from typing import List

from src.consumer.BreadCrumb import BreadCrumb
from src.postgres.PostGresDBEngineFactory import PostGresDBEngineFactory
from src.postgres.models import Base, Vehicle, Trip, Stop


class BreadCrumbRepository:
    _logger = logging.getLogger(__name__)

    def __init__(self):
        self._engine = PostGresDBEngineFactory.get_engine()
        Base.metadata.create_all(self._engine)

    def bulk_save_breadcrumbs(self, breadcrumbs: List[BreadCrumb]):
        if breadcrumbs:
            self._logger.info('Bulk saving {} breadcrumb records ...'.format(len(breadcrumbs)))

            with Session(self._engine, autoflush=False) as session:

                vehicle_ids = {breadcrumb.vehicle_id for breadcrumb in breadcrumbs}
                session.execute(insert(Vehicle).values([dict(id=vehicle_id) for vehicle_id in vehicle_ids]).on_conflict_do_nothing())

                trips = {(breadcrumb.event_no_trip, breadcrumb.vehicle_id, breadcrumb.opd_date) for breadcrumb in breadcrumbs}
                session.execute(insert(Trip).values([dict(id=trip[0], vehicle_id=trip[1], date=trip[2]) for trip in trips]).on_conflict_do_nothing())

                stops = {(breadcrumb.event_no_stop, breadcrumb.event_no_trip) for breadcrumb in breadcrumbs}
                session.execute(insert(Stop).values([dict(id=stop[0], trip_id=stop[1]) for stop in stops]).on_conflict_do_nothing())

                session.bulk_save_objects([breadcrumb.get_event() for breadcrumb in breadcrumbs])

                session.commit()
            self._logger.info('Bulk save complete!')

    def save_breadcrumb(self, breadcrumb: BreadCrumb):
        with Session(self._engine, autoflush=False) as session:
            self._check_and_add(session, select(Vehicle).where(Vehicle.id == breadcrumb.vehicle_id), lambda: breadcrumb.get_vehicle())
            self._check_and_add(session, select(Trip).where(Trip.id == breadcrumb.event_no_trip), lambda: breadcrumb.get_trip())
            self._check_and_add(session, select(Stop).where(Stop.id == breadcrumb.event_no_stop), lambda: breadcrumb.get_stop())
            session.add(breadcrumb.get_event())

            session.commit()

            return session

    @staticmethod
    def _check_and_add(session, check_statement, model_obj_generator):
        if session.execute(check_statement).scalar_one_or_none():
            return
        else:
            session.add(model_obj_generator())
            session.flush()
