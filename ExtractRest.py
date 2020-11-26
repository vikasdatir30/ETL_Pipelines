import requests as req
from datetime import datetime as dt
from datetime import timezone
#from googletrans import Translator
import pandas as pd
import numpy as np
import time
import schedule
import json as js


class ExtractRest:
    def __init__(self):
        try:
            #setting timestamp
            self.ingest_time = dt.now().strftime('%y-%m-%d %H:%M:%S')
            self.timestamp_utc = str(dt.now(timezone.utc))
            self.row_count=0

            #connection initialization
            self.url= "https://www.svivaaqm.net:44301/v1/envista/stations?Authorization"
            self.header = {
                'Authorization': 'ApiToken 71e67c41-8478-4310-9293-196f559493ca',
                'envi-data-source': 'MANA'}
            self.resp = req.get(self.url, headers=self.header, verify=False)

            # setting file variable with timestamp part
            self.out_file = 'ENVISTA_STATIONS_' + str(self.ingest_time).replace(' ', '_') + '.csv'

            #setting translator object
            #self.lang_Trans = Translator()
            print('Connected..!')
        except req.HTTPError as err:
            print(err)

    def trsnform(self):
        try:
            #creating dataframe object
            row_hdr=['ingestion_time','timestamp_utc','latitude','longitude','station_name','station_id','station_type','station_flag','station_status','altitude','temperature','precipitation_r','precipitation_s','humidity','wind_speed','wind_direction','wind_gust','dew_point','baro_pressure_sl','baro_pressure_al','baro_pressure_sf','pm25','pm10','no2','o3','so2','co','aqi']
            df = pd.DataFrame(columns=row_hdr)
            df.to_csv(self.out_file, index=False)

            ingestion_time = []
            timestamp_utc = []
            latitude = []
            longitude = []
            station_name = []
            station_id = []
            station_type = []
            station_flag = []
            station_status = []
            altitude = []
            temperature = []
            precipitation_r = []
            precipitation_s = []
            humidity = []
            wind_speed = []
            wind_direction = []
            wind_gust = []
            dew_point = []
            baro_pressure_sl = []
            baro_pressure_al = []
            baro_pressure_sf = []
            pm25 = []
            pm10 = []
            no2 = []
            o3 = []
            so2 = []
            co = []
            aqi = []
            row_count = 0
            raw_data = js.loads(self.resp.text)
            for stn in raw_data:
                row_count += 1
                ingestion_time.append(self.ingest_time)
                timestamp_utc.append(self.timestamp_utc)
                if stn['location']['latitude'] != None:
                    latitude.append(float(stn['location']['latitude']))
                else:
                    latitude.append(00.00)

                if stn['location']['longitude'] != None:
                    longitude.append(float(stn['location']['longitude']))
                else:
                    longitude.append(00.00)

                station_name.append(stn['name'])
                # station_name.append(self.lang_Trans.translate(stn['name'], src='he', dest='en'))

                station_id.append(int(stn['stationId']))
                station_type.append('null')
                station_flag.append(stn['stationsTag'].replace('(null)', 'null'))
                station_status.append(stn['active'])
                altitude.append(float(0))
                for monitor in stn['monitors']:
                    if monitor['pollutantId'] != None:
                        if monitor['name'].strip() == 'Temp':
                            temperature.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'Rain':
                            precipitation_r.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'NOX':
                            precipitation_s.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'RH':
                            humidity.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'WS':
                            wind_speed.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'WD':
                            wind_direction.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'PM2.5':
                            pm25.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'PM10':
                            pm10.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'NO2':
                            no2.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'O3':
                            o3.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'SO2':
                            so2.append(float(monitor['pollutantId']))

                        if monitor['name'].strip() == 'CO':
                            co.append(float(monitor['pollutantId']))

                # setting 0 to unavaliable values
                wind_gust.append(float(0))
                dew_point.append(float(0))
                baro_pressure_sl.append(float(0))
                baro_pressure_al.append(float(0))
                baro_pressure_sf.append(float(0))
                aqi.append(float(0))

                # loading data using dataframe
                df = pd.DataFrame({'ingestion_time': pd.Series(ingestion_time), 'timestamp_utc': pd.Series(timestamp_utc),
                               'latitude': pd.Series(latitude, dtype=np.float),'longitude': pd.Series(longitude, dtype=np.float), 'station_name': pd.Series(station_name),
                               'station_id': pd.Series(station_id), 'station_type': pd.Series(station_type),
                               'station_flag': pd.Series(station_flag), 'station_status': pd.Series(station_status),
                               'altitude': pd.Series(altitude,dtype=np.float),'temperature': pd.Series(temperature,dtype=np.float), 'precipitation_r': pd.Series(precipitation_r,dtype=np.float),
                               'precipitation_s': pd.Series(precipitation_s,dtype=np.float), 'humidity': pd.Series(humidity,dtype=np.float),
                               'wind_speed': pd.Series(wind_speed,dtype=np.float),'wind_direction': pd.Series(wind_direction,dtype=np.float), 'wind_gust': pd.Series(wind_gust,dtype=np.float),
                               'dew_point': pd.Series(dew_point), 'baro_pressure_sl': pd.Series(baro_pressure_sl,dtype=np.float),
                               'baro_pressure_al': pd.Series(baro_pressure_al,dtype=np.float),'baro_pressure_sf': pd.Series(baro_pressure_sf,dtype=np.float), 'pm25': pd.Series(pm25,dtype=np.float),
                               'pm10': pd.Series(pm10,dtype=np.float),'no2': pd.Series(no2,dtype=np.float), 'o3': pd.Series(o3,dtype=np.float), 'so2': pd.Series(so2,dtype=np.float), 'co': pd.Series(co,dtype=np.float),
                               'aqi': pd.Series(aqi,dtype=np.float)}, columns=row_hdr)
                df.to_csv(self.out_file, sep=',', encoding='UTF-8', mode='a', header=False, index=False , na_rep='NULL')

                #clear value holders
                ingestion_time.clear()
                timestamp_utc.clear()
                latitude.clear()
                longitude.clear()
                station_name.clear()
                station_id.clear()
                station_type.clear()
                station_flag.clear()
                station_status.clear()
                altitude.clear()
                temperature.clear()
                precipitation_r.clear()
                precipitation_s.clear()
                humidity.clear()
                wind_speed.clear()
                wind_direction.clear()
                wind_gust.clear()
                dew_point.clear()
                baro_pressure_sl.clear()
                baro_pressure_al.clear()
                baro_pressure_sf.clear()
                pm25.clear()
                pm10.clear()
                no2.clear()
                o3.clear()
                so2.clear()
                co.clear()
                aqi.clear()

            print(str(row_count) + " Rows have been processed. Output_File : " + self.out_file)

            # closing connection
            self.resp.close()
            print('Disconnected..!')
        except Exception as err:
            print(str(err), self.row_count)


#calling pipeline
def rest_pipeline():
    obj=ExtractRest()
    obj.trsnform()
    del obj


#Schedule for every 10 min
schedule.every(10*60).seconds.do(rest_pipeline)

while True:
    schedule.run_pending()
    time.sleep(1)


