from unittest import TestCase

from datetime import date

from src.consumer.BreadCrumb import BreadCrumb


class TestBreadCrumb(TestCase):

    def test_load_from_json(self):
        json = """
        {
            "EVENT_NO_TRIP": "166947931",
            "EVENT_NO_STOP": "166947932",
            "OPD_DATE": "02-SEP-20",
            "VEHICLE_ID": "1776",
            "METERS": "1658",
            "ACT_TIME": "21625",
            "VELOCITY": "4",
            "DIRECTION": "270",
            "RADIO_QUALITY": "",
            "GPS_LONGITUDE": "-122.600003",
            "GPS_LATITUDE": "45.642963",
            "GPS_SATELLITES": "12",
            "GPS_HDOP": "0.7",
            "SCHEDULE_DEVIATION": "18"
          }
        """

        bread_crumb = BreadCrumb.parse_raw(json)

        self.assertIsNotNone(bread_crumb)

        self.assertEqual(bread_crumb.event_no_trip, 166947931)
        self.assertEqual(bread_crumb.event_no_stop, 166947932)
        self.assertEqual(bread_crumb.opd_date, date(2020, 9, 2))
        self.assertEqual(bread_crumb.vehicle_id, 1776)
        self.assertEqual(bread_crumb.meters, 1658)
        self.assertEqual(bread_crumb.act_time, 21625)
        self.assertEqual(bread_crumb.velocity, 4)
        self.assertEqual(bread_crumb.direction, 270)
        self.assertEqual(bread_crumb.radio_quality, "")
        self.assertEqual(bread_crumb.gps_longitude, -122.600003)
        self.assertEqual(bread_crumb.gps_latitude, 45.642963)
        self.assertEqual(bread_crumb.gps_satellites, 12)
        self.assertEqual(bread_crumb.gps_hdop, 0.7)
        self.assertEqual(bread_crumb.schedule_deviation, 18)
