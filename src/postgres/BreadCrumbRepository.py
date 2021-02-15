import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from typing import List

from src.consumer.BreadCrumb import BreadCrumb
from src.postgres.PostGresDBEngineFactory import PostGresDBEngineFactory
from src.postgres.models.models import Base, Trip


class BreadCrumbRepository:
    _logger = logging.getLogger(__name__)

    def __init__(self):
        self._engine = PostGresDBEngineFactory.get_engine()
        Base.metadata.create_all(self._engine)

    def bulk_save_breadcrumbs(self, breadcrumbs: List[BreadCrumb]):
        if breadcrumbs:
            self._logger.info('Bulk saving {} breadcrumb records ...'.format(len(breadcrumbs)))

            unique_trips = {(breadcrumb.event_no_trip, breadcrumb.vehicle_id) for breadcrumb in breadcrumbs}

            with Session(self._engine, autoflush=False) as session:
                session.execute(insert(Trip).values([dict(trip_id=trip[0], vehicle_id=trip[1]) for trip in unique_trips]).on_conflict_do_nothing())
                session.bulk_save_objects([breadcrumb.get_bread_crumb() for breadcrumb in breadcrumbs])

                session.commit()
            self._logger.info('Bulk save complete!')
