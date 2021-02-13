import json
import logging
import os

class Validator:

#data = {"EVENT_NO_TRIP": "168515951", "EVENT_NO_STOP": "168515955", "OPD_DATE": "22-SEP-20", "VEHICLE_ID": "1254300", "METERS": "216970", "ACT_TIME": "61131", "VELOCITY": "28", "DIRECTION": "109", "RADIO_QUALITY": "", "GPS_LONGITUDE": "-122.527272", "GPS_LATITUDE": "45.597937", "GPS_SATELLITES": "12", "GPS_HDOP": "0.7", "SCHEDULE_DEVIATION": "-455"}

month_dict = {"00": "00", "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}

  @classmethod
  def transform_data(data):
      """
      :return: breadcrumb data with any neccessary changes made to its data in order to move data to validation
      """
      print('Breadcrumb data received: ' + str(data))
      # Assert Direction is in range 0-359, if not set to (-1)
      print(data['DIRECTION'])
      if int(data['DIRECTION']) < 0 or int(data['DIRECTION']) > 359:
        print("DIRECTION ERROR")
        data['DIRECTION'] = "-1"

      # Change months to numerical value, set date to default 00-00-0000 if value empty
      if data['OPD_DATE'] == "":
        print("OPD_DATE ERROR")
        data['OPD_DATE'] = "00-00-0000"
      else:
        date = (data['OPD_DATE'].split('-'))
        date[1] = month_dict[date[1]]
        date[2] = "20" + str(date[2])
        data['OPD_DATE'] = ('-').join(date)

      # If ACT_TIME is not between 14867 and 90521, set to -1 (error)
      if int(data['ACT_TIME']) < 14867 or int(data['ACT_TIME']) > 90521:
        print("ACT_TIME ERROR")
        data['ACT_TIME'] = "-1"

      # If GPS_SATELLITE value is not 10, 11, or 12, set to -1 (error)
      if int(data['GPS_SATELLITES']) < 10 or int(data['GPS_SATELLITES']) > 12:
        print("GPS_SATELLITES ERROR")
        data['GPS_SATELLITES'] = "-1"

      # Set GPS_LONG and GPS_LAT to correct whole number values of -122 and 45, respectively, or "-1" if empty
      if data['GPS_LONGITUDE'] == "":
        print("GPS_LONG EMPTY")
        data['GPS_LONGITUDE'] = "-1"
      elif not data['GPS_LONGITUDE'][:4] == "-122":
        print("GPS_LONG ERROR")
        data['GPS_LONGITUDE'] = "-122.00"

      if data['GPS_LATITUDE'] == "":
        print("GPS_LAT EMPTY")
        data['GPS_LATITUDE'] = "-1"
      elif not data['GPS_LATITUDE'][:2] == "45":
        print("GPS_LAT ERROR")
        data['GPS_LATITUDE'] = "45.00"

      return data

