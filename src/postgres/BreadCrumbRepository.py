import logging
from typing import List

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.consumer.BreadCrumb import BreadCrumb
from src.postgres.PostGresDBEngineFactory import PostGresDBEngineFactory
from src.postgres.models.ServiceType import ServiceType
from src.postgres.models.TripDirType import TripDirType
from src.postgres.models.models import Base, Trip


class BreadCrumbRepository:
    _logger = logging.getLogger(__name__)

    def __init__(self):
        self._engine = PostGresDBEngineFactory.get_engine()
        Base.metadata.create_all(self._engine)

    def bulk_save_breadcrumbs(self, breadcrumbs: List[BreadCrumb], trips_stop_data):
        if breadcrumbs:
            self._logger.info('Bulk saving {} breadcrumb records ...'.format(len(breadcrumbs)))

            unique_trips = {(breadcrumb.event_no_trip, breadcrumb.vehicle_id) for breadcrumb in breadcrumbs}

            with Session(self._engine, autoflush=False) as session:
                trips = [self._get_trips_dict(trip[0], trip[1], trips_stop_data) for trip in unique_trips]

                session.execute(insert(Trip).values(trips).on_conflict_do_nothing())
                session.bulk_save_objects([breadcrumb.get_bread_crumb() for breadcrumb in breadcrumbs])

                session.commit()
            self._logger.info('Bulk save complete!')

    @staticmethod
    def _get_trips_dict(trip_id, vehicle_id, trips_stop_data):
        if str(trip_id) in trips_stop_data.keys():

            trip_stop = trips_stop_data[str(trip_id)]
            assert trip_stop, 'No trip stop found for trip ID ' + trip_id

            direction = BreadCrumbRepository._get_direction(trip_stop)

            return dict(trip_id=trip_id,
                        route_id=trip_stop['route_id'].item(),
                        vehicle_id=vehicle_id,
                        service_key=trip_id._get_service_key(trip_stop),
                        direction=direction
                        )
        else:
            return dict(trip_id=trip_id,
                        route_id=None,
                        vehicle_id=vehicle_id,
                        service_key=None,
                        direction=None
                        )

    @staticmethod
    def _get_service_key(trip_stop):
        if trip_stop['direction'] == 'U':
            return ServiceType.Sunday.value
        elif trip_stop['direction'] == 'W':
            return ServiceType.Weekday.value
        else:
            return ServiceType.Saturday.value

    @staticmethod
    def _get_direction(trip_stop):
        if trip_stop['direction'].item() == 0 or trip_stop['direction'].item() == 1:
            direction = TripDirType.Back.value if trip_stop['direction'].item() else TripDirType.Out.value
        else:
            direction = None
        return direction
