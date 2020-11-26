from ftplib import all_errors
from ftplib import FTP
from datetime import datetime as dt
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
import time
import schedule

class ExtractXML:
    def __init__(self, ftp_loc="ftp://ftp2.bom.gov.au/anon/gen/fwo/IDN60920.xml"):
        try:
            # setting timestamp
            self.ingest_time = dt.now().strftime('%y-%m-%d %H:%M:%S')
            self.row_count=0

            #self.ftp_loc=ftp_loc.split('//')[1].split('/')[0]
            self.ftp_loc='ftp2.bom.gov.au'
            self.ftp_src_dir='/anon/gen/fwo/'
            self.ftp_src_file='IDN60920.xml'

            #Conneting to FTP server
            self.ftp_conect = FTP(self.ftp_loc)
            if (self.ftp_conect.login()):
                print('Connected.. !')

            #Stage and out file variables
            self.stg_file='STG_'+self.ftp_src_file
            self.out_file=self.ftp_src_file.split(".")[0]+'_'+str(self.ingest_time).replace(' ', '_')+'.csv'


            #Downloading file from ftp location
            self.ftp_conect.cwd(self.ftp_src_dir)
            with open(self.stg_file, 'wb') as fptr:
                self.ftp_conect.retrbinary('RETR ' + self.ftp_src_file,
                                           fptr.write,
                                           1024)
            print(self.stg_file +' has downloaded..!')

            self.ftp_conect.close()
            print('Disconnected..!')

        except all_errors as e:
            print(str(e))

    def transform(self):
        try:
            #creating dataframe object
            row_hdr=['ingestion_time','timestamp_utc','latitude','longitude','station_name','station_id','station_type','station_flag','station_status','altitude','temperature','precipitation_r','precipitation_s','humidity','wind_speed','wind_direction','wind_gust','dew_point','baro_pressure_sl','baro_pressure_al','baro_pressure_sf','pm25','pm10','no2','o3','so2','co','aqi']
            df = pd.DataFrame(columns=row_hdr)
            df.to_csv(self.out_file, index=False)

            # setting value holders
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

            #starts iteration
            raw_data= et.parse(self.stg_file)
            root = raw_data.getroot()
            stations = [chd for chd in root[1]]
            for stn in stations:

                self.row_count+=1

                ingestion_time.append(self.ingest_time)
                latitude.append(float(stn.attrib['lat']))
                longitude.append(float(stn.attrib['lon']))
                station_name.append(stn.attrib['stn-name'])
                station_id.append(stn.attrib['wmo-id'])
                station_type.append(stn.attrib['type'])
                altitude.append(stn.attrib['stn-height'])

                for per in stn.iter('period'):
                    timestamp_utc.append(per.attrib['time-utc'])
                for elmts in stn.iter('level'):
                    for elmt in elmts:

                        #temperature
                        if elmt.attrib['type'] == 'apparent_temp':
                            temperature.append(float(elmt.text))

                        #precipitation_r - rain
                        if elmt.attrib['type']=="rainfall":
                            precipitation_r.append(float(elmt.text))

                        # precipitation_s - now
                        if elmt.attrib['type'] == "rainfall_24hr":
                            precipitation_s.append(float(elmt.text))

                        #humidity
                        if elmt.attrib['type']=="rel-humidity":
                            humidity.append(float(elmt.text))

                        #WIND SPEED
                        if elmt.attrib['type']=="wind_spd":
                            wind_speed.append(float(elmt.text))

                        #WIND DIR
                        if elmt.attrib['type']=="wind_dir_deg":
                            wind_direction.append(float(elmt.text))

                        #WIND GUST
                        if elmt.attrib['type']=="wind_gust_spd":
                            wind_gust.append(float(elmt.text))

                        #dew_point
                        if elmt.attrib['type']=="dew_point":
                            dew_point.append(float(elmt.text))

                        #BAROMETRIC_PRESSURE_SL
                        if elmt.attrib['type']=="msl_pres":
                            baro_pressure_sl.append(float(elmt.text))

                        #BAROMETRIC_PRESSURE_AL
                        if elmt.attrib['type']=="pres":
                            baro_pressure_al.append(float(elmt.text))

                        #BAROMETRIC_PRESSURE_SF
                        if elmt.attrib['type']=="qnh_pres":
                            baro_pressure_sf.append(float(elmt.text))

                    #setting unavailable values to null
                    pm25.append(float(0))
                    pm10.append(float(0))
                    no2.append(float(0))
                    o3.append(float(0))
                    so2.append(float(0))
                    co.append(float(0))
                    aqi.append(float(0))
                    station_flag.append(float(0))
                    station_status.append(float(0))
                df = pd.DataFrame(
                        {'ingestion_time': pd.Series(ingestion_time), 'timestamp_utc': pd.Series(timestamp_utc),
                         'latitude': pd.Series(latitude, dtype=np.float),
                         'longitude': pd.Series(longitude, dtype=np.float), 'station_name': pd.Series(station_name),
                         'station_id': pd.Series(station_id), 'station_type': pd.Series(station_type),
                         'station_flag': pd.Series(station_flag), 'station_status': pd.Series(station_status),
                         'altitude': pd.Series(altitude, dtype=np.float),
                         'temperature': pd.Series(temperature, dtype=np.float),
                         'precipitation_r': pd.Series(precipitation_r, dtype=np.float),
                         'precipitation_s': pd.Series(precipitation_s, dtype=np.float),
                         'humidity': pd.Series(humidity, dtype=np.float),
                         'wind_speed': pd.Series(wind_speed, dtype=np.float),
                         'wind_direction': pd.Series(wind_direction, dtype=np.float),
                         'wind_gust': pd.Series(wind_gust, dtype=np.float),
                         'dew_point': pd.Series(dew_point, dtype=np.float),
                         'baro_pressure_sl': pd.Series(baro_pressure_sl, dtype=np.float),
                         'baro_pressure_al': pd.Series(baro_pressure_al, dtype=np.float),
                         'baro_pressure_sf': pd.Series(baro_pressure_sf, dtype=np.float),
                         'pm25': pd.Series(pm25, dtype=np.float),
                         'pm10': pd.Series(pm10, dtype=np.float), 'no2': pd.Series(no2, dtype=np.float),
                         'o3': pd.Series(o3, dtype=np.float), 'so2': pd.Series(so2, dtype=np.float),
                         'co': pd.Series(co, dtype=np.float),
                         'aqi': pd.Series(aqi, dtype=np.float)}, columns=row_hdr)
                #writer
                df.to_csv(self.out_file, sep=',', encoding='UTF-8', mode='a', header=False, index=False,na_rep='NULL')

                # clear value holders
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

            print(str(self.row_count) + " Rows have been processed. Output_File : " + self.out_file)
        except Exception as err:
            print(str(err))

def xml_pipeline():
    obj=ExtractXML()
    obj.transform()
    del obj

#Schedule for every 10 min
schedule.every(10*60).seconds.do(xml_pipeline)
while True:
    schedule.run_pending()
    time.sleep(1)