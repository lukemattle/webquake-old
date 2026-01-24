import dmdws, time, os, json, db, threading, requests
from enums import eng_codes
from flask import Flask
from flask_sock import Sock
from simple_websocket.ws import Base as WebsocketBase


# Creating the connection object
conn = dmdws.Connection(
    refresh_token="<insert your refresh token here>",
    types=[
        "VXSE43", # Earthquake early warning (warning) (5- and above) (predicted)
        "VXSE45", # Earthquake early warning (seismic motion forecast) (below 5- intensity) (done)
        "VXSE47", # Real-time seismic intensity only used for PLUM (predicted)
        "VXSE51", # Earthquake intensity report (done)
        "VXSE52", # Earthquake information (information about the epicenter) (done)
        "VXSE53", # Earthquake information (information regarding the epicenter and intensity) (done)
        "VTSE41", # Tsunami warning/advisory/forecast (done)
        "VTSE51", # Tsunami information (done)
        "VTSE52" # Tsunami information (information regarding offshore tsunami observation)
    ],
    debug=True # If you want to see the debug messages, disable in production
)

def process_data(data): # Extracts the relevant information from the data
    output_data = {}
    if data['head']['type'] == 'VXSE53': # Earthquake information (information regarding the epicenter and intensity)
        epi_location_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('code', ''), 'Unknown')] # translates the region code to English
        epi_location_jp = [data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('name', 'Unknown')] # .get is used to avoid errors if the key is not present
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z"))) # converts the time to a Unix timestamp
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('to', 0)
        if max_int == 'over': # sometimes the intensity is given as 'over', in which case the actual value is given from 'from'
            max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('from', 0)
        max_lpgm = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxLgInt', {}).get('to', 0)
        magnitude = data.get('body', {}).get('body', {}).get('earthquake', {}).get('magnitude', {}).get('value', 0.0)
        depth = data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('depth', {}).get('value', 0)
        prefectures_en = [eng_codes.prefecture.get(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])[i].get('code', ''), 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])))] # loops through all prefectures
        prefectures_jp = [data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])[i].get('name', 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])))]
        regions_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])[i].get('code', ''), 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])))]
        regions_jp = [data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])[i].get('name', 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])))]
        if "0215" in data.get('body', {}).get('body', {}).get('comments', {}).get('forecast', {}).get('codes', []):
            no_tsunami = True 
        else:
            no_tsunami = False
        output_data = { # stores the data in a dictionary
            'type': 'earthquake',
            'epi_location_en': epi_location_en,
            'epi_location_jp': epi_location_jp,
            'report_time': report_time,
            'quake_time': quake_time,
            'max_int': max_int,
            'max_lpgm': max_lpgm,
            'magnitude': magnitude,
            'depth': depth,
            'prefectures_en': prefectures_en,
            'prefectures_jp': prefectures_jp,
            'regions_en': regions_en,
            'regions_jp': regions_jp,
            'no_tsunami': no_tsunami
        }
        dmdws.logger.info('Data (VXSE53) processed at', time.time())

    elif data['head']['type'] == 'VXSE45': # Earthquake early warning (seismic motion forecast) (below 5- intensity)
        epi_location_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('code', ''), 'Unknown')] 
        epi_location_jp = [data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('name', 'Unknown')] 
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        report_num = data.get('body', {}).get('serialNo', 0)
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('to', 0)
        if max_int == 'over':
            max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('from', 0)
        max_lpgm = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxLgInt', {}).get('to', 0)
        last_report = data.get('body', {}).get('body', {}).get('isLastInfo', False)
        magnitude = data.get('body', {}).get('body', {}).get('earthquake', {}).get('magnitude', {}).get('value', 0.0) 
        depth = data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('depth', {}).get('value', 0) 
        sea = True if data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('landOrSea', '') == "海域" else False 
        warning = True if data.get('body', {}).get('body', {}).get('isWarning', '') == "true" else False
        if bool(sea) == True and float(magnitude) >= 6.0 and float(depth) <= 30 or float(magnitude) >= 7.0 and float(depth) <= 60 or float(magnitude) >= 8.0: # algorithm for determining if a tsunami is possible
            tsunami_possible = True
        else:
            tsunami_possible = False
        output_data = {
            'type': 'earthquake',
            'last_report': last_report,
            'epi_location_en': epi_location_en,
            'epi_location_jp': epi_location_jp,
            'report_time': report_time,
            'report_num': report_num,
            'quake_time': quake_time,
            'max_int': max_int,
            'max_lpgm': max_lpgm,
            'magnitude': magnitude,
            'depth': depth,
            'sea': sea,
            'warning': warning,
            'tsunami_possible': tsunami_possible
        }
        dmdws.logger.info('Data (VXSE45) processed at', time.time())

    elif data['head']['type'] == 'VXSE43': # PREDICTION - Earthquake early warning (warning) (5- and above)
        epi_location_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('code', ''), 'Unknown')]
        epi_location_jp = [data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('name', 'Unknown')]
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        report_num = data.get('body', {}).get('serialNo', 0)
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('to', 0)
        if max_int == 'over':
            max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('from', 0)
        max_lpgm = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxLgInt', {}).get('to', 0)
        last_report = data.get('body', {}).get('body', {}).get('isLastInfo', False)
        magnitude = data.get('body', {}).get('body', {}).get('earthquake', {}).get('magnitude', {}).get('value', 0.0)
        depth = data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('depth', {}).get('value', 0)
        sea = True if data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('landOrSea', '') == "海域" else False
        warning = True if data.get('body', {}).get('body', {}).get('isWarning', '') == "true" else False
        if bool(sea) == True and float(magnitude) >= 6.0 and float(depth) <= 30 or float(magnitude) >= 7.0 and float(depth) <= 60 or float(magnitude) >= 8.0: # algorithm for determining if a tsunami is possible
            tsunami_possible = True
        else:
            tsunami_possible = False
        output_data = {
            'type': 'earthquake',
            'last_report': last_report,
            'epi_location_en': epi_location_en,
            'epi_location_jp': epi_location_jp,
            'report_time': report_time,
            'report_num': report_num,
            'quake_time': quake_time,
            'max_int': max_int,
            'max_lpgm': max_lpgm,
            'magnitude': magnitude,
            'depth': depth,
            'sea': sea,
            'warning': warning,
            'tsunami_possible': tsunami_possible
        }
        dmdws.logger.info('Data (VXSE43) processed at', time.time())

    elif data['head']['type'] == 'VXSE47': # PREDICTION - Real-time seismic intensity only used for PLUM
        epi_location_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('code', ''), 'Unknown')]
        epi_location_jp = [data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('name', 'Unknown')]
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        report_num = data.get('body', {}).get('serialNo', 0)
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('to', 0)
        if max_int == 'over':
            max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxInt', {}).get('from', 0)
        max_lpgm = data.get('body', {}).get('body', {}).get('intensity', {}).get('forecastMaxLgInt', {}).get('to', 0)
        last_report = data.get('body', {}).get('body', {}).get('isLastInfo', False)
        magnitude = data.get('body', {}).get('body', {}).get('earthquake', {}).get('magnitude', {}).get('value', 0.0)
        depth = data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('depth', {}).get('value', 0)
        sea = True if data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('landOrSea', '') == "海域" else False
        warning = True if data.get('body', {}).get('body', {}).get('isWarning', '') == "true" else False
        if bool(sea) == True and float(magnitude) >= 6.0 and float(depth) <= 30 or float(magnitude) >= 7.0 and float(depth) <= 60 or float(magnitude) >= 8.0:
            tsunami_possible = True
        else:
            tsunami_possible = False
        output_data = {
            'type': 'earthquake',
            'warning': False,
            'last_report': last_report,
            'epi_location_en': epi_location_en,
            'epi_location_jp': epi_location_jp,
            'report_time': report_time,
            'report_num': report_num,
            'quake_time': quake_time,
            'max_int': max_int,
            'max_lpgm': max_lpgm,
            'magnitude': magnitude,
            'depth': depth,
            'sea': sea,
            'warning': warning,
            'tsunami_possible': tsunami_possible
        }
        dmdws.logger.info('Data (VXSE45) processed at', time.time())

    elif data['head']['type'] == 'VXSE51': # Earthquake intensity report
        max_int = data.get('body', {}).get('body', {}).get('intensity', {}).get('maxInt', 0)
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        prefectures_en = [eng_codes.prefecture.get(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])[i].get('code', ''), 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])))]
        prefectures_jp = [data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])[i].get('name', 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('prefectures', [])))]
        regions_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])[i].get('code', ''), 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])))]
        regions_jp = [data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])[i].get('name', 'Unknown') 
                for i in range(len(data.get('body', {}).get('body', {}).get('intensity', {}).get('regions', [])))]
        output_data = {
            'type': 'earthquake',
            'max_int': max_int,
            'report_time': report_time,
            'quake_time': quake_time,
            'prefectures_en': prefectures_en,
            'prefectures_jp': prefectures_jp,
            'regions_en': regions_en,
            'regions_jp': regions_jp
        }
        dmdws.logger.info('Data (VXSE51) processed at', time.time())

    elif data['head']['type'] == 'VXSE52': # Earthquake information (information about the epicenter)
        epi_location_en = [eng_codes.regions.get(data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('code', ''), 'Unknown')]
        epi_location_jp = [data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('name', 'Unknown')]
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        magnitude = data.get('body', {}).get('body', {}).get('earthquake', {}).get('magnitude', {}).get('value', 0.0)
        depth = data.get('body', {}).get('body', {}).get('earthquake', {}).get('hypocenter', {}).get('depth', {}).get('value', 0)
        if '0215' in data.get('body', {}).get('body', {}).get('comments', {}).get('forecast', {}).get('codes', []): # 0215 is the code for no tsunami
            no_tsunami = True
        else:
            no_tsunami = False
        output_data = {
            'type': 'earthquake',
            'epi_location_en': epi_location_en,
            'epi_location_jp': epi_location_jp,
            'report_time': report_time,
            'quake_time': quake_time,
            'magnitude': magnitude,
            'depth': depth,
            'no_tsunami': no_tsunami
        }
        dmdws.logger.info('Data (VXSE52) processed at', time.time())

    elif data['head']['type'] == 'VTSE41': # Tsunami warning/advisory/forecast
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        regions_en = [eng_codes.coastal_regions.get(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('code', ''), 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        regions_jp = [data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('name', 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        heights = [float(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('maxHeight', {}).get('height', {}).get('value', 0))
               for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        max_height = max(heights, default=0) # finds the maximum height of the tsunami from the heights list
        if max_height == 0.2: # determines the warning level based on the height
            warning_level = 'Slight sea level change'
        elif max_height <= 1:
            warning_level = 'Advisory'
        elif 1 < max_height <= 3:
            warning_level = 'Warning'
        else:
            warning_level = 'Major Warning'

        output_data = {
            'type': 'tsunami',
            'report_time': report_time,
            'quake_time': quake_time,
            'regions_en': regions_en,
            'regions_jp': regions_jp,
            'heights': heights,
            'obs_regions_en': obs_regions_en,
            'obs_regions_jp': obs_regions_jp,
            'obs_heights': obs_heights,
            'warning_level': warning_level
        }
        dmdws.logger.info('Data (VTSE41) processed at', time.time())

    elif data['head']['type'] == 'VTSE51': # Tsunami information
        report_time = int(time.mktime(time.strptime(data.get('body', {}).get('reportDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        quake_time = int(time.mktime(time.strptime(data.get('body', {}).get('targetDateTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
        regions_en = [eng_codes.coastal_regions.get(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('code', ''), 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        regions_jp = [data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('name', 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        heights = [float(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('maxHeight', {}).get('height', {}).get('value', 0))
               for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        arrival_times = [int(time.mktime(time.strptime(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])[i].get('firstHeight', '').get('arrivalTime', ''), "%Y-%m-%dT%H:%M:%S%z")))
                for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('forecasts', [])))]
        obs_regions_en = [eng_codes.coastal_regions.get(data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])[i].get('code', ''), 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])))]
        obs_regions_jp = [data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])[i].get('name', 'Unknown')
                  for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])))]
        obs_heights = [float(data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])[i].get('maxHeight', {}).get('height', {}).get('value', 0))
                   for i in range(len(data.get('body', {}).get('body', {}).get('tsunami', {}).get('observations', [])))]
        max_height = max(heights, default=0)
        if max_height == 0.2:
            warning_level = 'Slight sea level change'
        elif max_height <= 1:
            warning_level = 'Advisory'
        elif 1 < max_height <= 3:
            warning_level = 'Warning'
        else:
            warning_level = 'Major Warning'

        output_data = {
            'type': 'tsunami',
            'report_time': report_time,
            'quake_time': quake_time,
            'regions_en': regions_en,
            'regions_jp': regions_jp,
            'heights': heights,
            'arrival_times': arrival_times,
            'obs_regions_en': obs_regions_en,
            'obs_regions_jp': obs_regions_jp,
            'obs_heights': obs_heights,
            'warning_level': warning_level
        }
        dmdws.logger.info('Data (VTSE51) processed at', time.time())

    else:
        dmdws.logger.info('Data type not recognised at', time.time())
    return output_data

# will be called when a message is received
def on_message(message: dict):
    dmdws.logger.info(message['type']) # Type of the message ('start' or 'data') (you can ignore 'start' messages, they will happen on a new websocket connection)
    
    if message['type'] == 'data':
        dmdws.logger.info(message['head']['type']) # Contains the code type ('VXSE53', etc)
        dmdws.logger.info(message['body']) # Body of the message (contains the actual data)

        
        output_path = os.path.join('data_messages', message['head']['type'], f"{int(time.time())}.json")
        with open(output_path, 'w', encoding='utf-8') as outfile:
            json.dump(message, outfile, ensure_ascii=False, indent=4) # Save the raw data to a json file
        dmdws.logger.info('Data saved at', time.time()) 
        dmdws.logger.info(message['body'])

        output_data = process_data(message)
        output_path = os.path.join('decoded_messages', f"{int(time.time())}.json")
        with open(output_path, 'w', encoding='utf-8') as outfile:
            json.dump(output_data, outfile, ensure_ascii=False, indent=4) # Save the processed data to a json file
        dmdws.logger.info('Data saved at', time.time())
        dmdws.logger.info(message['body'])
        store_recent_data(output_data, message)
        send_alert(output_data)
        send_data_to_all_sockets(output_data)
        store_data(output_data, message)


# Creating directories to save the data
os.makedirs('data_messages', exist_ok=True)
os.makedirs('decoded_messages', exist_ok=True)
os.makedirs('logs', exist_ok=True)
for msg_type in ["VXSE43", "VXSE45", "VXSE47", "VXSE51", "VXSE52", "VXSE53", "VTSE41", "VTSE51", "VTSE52"]:
    os.makedirs(f'data_messages/{msg_type}', exist_ok=True)

# Create the table if it doesn't exist
# Create tables for each data type
with db.Database() as cursor:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE53 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epi_location_en TEXT,
            epi_location_jp TEXT,
            report_time INTEGER,
            quake_time INTEGER,
            max_int INTEGER,
            max_lpgm INTEGER,
            magnitude REAL,
            depth INTEGER,
            prefectures_en TEXT,
            prefectures_jp TEXT,
            regions_en TEXT,
            regions_jp TEXT,
            no_tsunami INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE45 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_report INTEGER,
            epi_location_en TEXT,
            epi_location_jp TEXT,
            report_time INTEGER,
            report_num INTEGER,
            quake_time INTEGER,
            max_int INTEGER,
            max_lpgm INTEGER,
            magnitude REAL,
            depth INTEGER,
            sea INTEGER,
            warning INTEGER,
            tsunami_possible INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE43 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_report INTEGER,
            epi_location_en TEXT,
            epi_location_jp TEXT,
            report_time INTEGER,
            report_num INTEGER,
            quake_time INTEGER,
            max_int INTEGER,
            max_lpgm INTEGER,
            magnitude REAL,
            depth INTEGER,
            sea INTEGER,
            warning INTEGER,
            tsunami_possible INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE47 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_report INTEGER,
            epi_location_en TEXT,
            epi_location_jp TEXT,
            report_time INTEGER,
            report_num INTEGER,
            quake_time INTEGER,
            max_int INTEGER,
            max_lpgm INTEGER,
            magnitude REAL,
            depth INTEGER,
            sea INTEGER,
            warning INTEGER,
            tsunami_possible INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE51 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            max_int INTEGER,
            report_time INTEGER,
            quake_time INTEGER,
            prefectures_en TEXT,
            prefectures_jp TEXT,
            regions_en TEXT,
            regions_jp TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VXSE52 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epi_location_en TEXT,
            epi_location_jp TEXT,
            report_time INTEGER,
            quake_time INTEGER,
            magnitude REAL,
            depth INTEGER,
            no_tsunami INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VTSE41 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_time INTEGER,
            quake_time INTEGER,
            regions_en TEXT,
            regions_jp TEXT,
            heights TEXT,
            warning_level TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VTSE51 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_time INTEGER,
            quake_time INTEGER,
            regions_en TEXT,
            regions_jp TEXT,
            heights TEXT,
            arrival_times TEXT,
            obs_regions_en TEXT,
            obs_regions_jp TEXT,
            obs_heights TEXT,
            warning_level TEXT
        )
    ''')

def store_data(output_data, data):
    # Determine the table based on the type
    table_name = f"{data['head']['type'].upper()}"
    dmdws.logger.info(f"Storing data in table: {table_name}")

    # Insert the values into the corresponding table
    if table_name == 'VXSE53':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE53 (
                    epi_location_en, epi_location_jp, report_time, quake_time, max_int, max_lpgm,
                    magnitude, depth, prefectures_en, prefectures_jp, regions_en, regions_jp, no_tsunami
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ', '.join(output_data.get('epi_location_en', [])) if output_data.get('epi_location_en') else None,
                ', '.join(output_data.get('epi_location_jp', [])) if output_data.get('epi_location_jp') else None,
                output_data.get('report_time', None), output_data.get('quake_time', None),
                output_data.get('max_int', None), output_data.get('max_lpgm', None),
                output_data.get('magnitude', None), output_data.get('depth', None),
                ', '.join(output_data.get('prefectures_en', [])) if output_data.get('prefectures_en') else None,
                ', '.join(output_data.get('prefectures_jp', [])) if output_data.get('prefectures_jp') else None,
                ', '.join(output_data.get('regions_en', [])) if output_data.get('regions_en') else None,
                ', '.join(output_data.get('regions_jp', [])) if output_data.get('regions_jp') else None,
                output_data.get('no_tsunami', None)
            ))
    elif table_name == 'VXSE45':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE45 (
                    last_report, epi_location_en, epi_location_jp, report_time, report_num, quake_time, max_int, max_lpgm,
                    magnitude, depth, sea, warning, tsunami_possible
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('last_report', None),
                ', '.join(output_data.get('epi_location_en', [])) if output_data.get('epi_location_en') else None,
                ', '.join(output_data.get('epi_location_jp', [])) if output_data.get('epi_location_jp') else None,
                output_data.get('report_time', None), output_data.get('report_num', None), output_data.get('quake_time', None),
                output_data.get('max_int', None), output_data.get('max_lpgm', None),
                output_data.get('magnitude', None), output_data.get('depth', None),
                output_data.get('sea', None),
                output_data.get('warning', None), output_data.get('tsunami_possible', None)
            ))
    elif table_name == 'VXSE43':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE43 (
                    last_report, epi_location_en, epi_location_jp, report_time, report_num, quake_time, max_int, max_lpgm,
                    magnitude, depth, sea, warning, tsunami_possible
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('last_report', None),
                ', '.join(output_data.get('epi_location_en', [])) if output_data.get('epi_location_en') else None,
                ', '.join(output_data.get('epi_location_jp', [])) if output_data.get('epi_location_jp') else None,
                output_data.get('report_time', None), output_data.get('report_num', None), output_data.get('quake_time', None),
                output_data.get('max_int', None), output_data.get('max_lpgm', None),
                output_data.get('magnitude', None), output_data.get('depth', None),
                output_data.get('sea', None),
                output_data.get('warning', None), output_data.get('tsunami_possible', None)
            ))
    elif table_name == 'VXSE47':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE47 (
                    last_report, epi_location_en, epi_location_jp, report_time, report_num, quake_time, max_int, max_lpgm,
                    magnitude, depth, sea, warning, tsunami_possible
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('last_report', None),
                ', '.join(output_data.get('epi_location_en', [])) if output_data.get('epi_location_en') else None,
                ', '.join(output_data.get('epi_location_jp', [])) if output_data.get('epi_location_jp') else None,
                output_data.get('report_time', None), output_data.get('report_num', None), output_data.get('quake_time', None),
                output_data.get('max_int', None), output_data.get('max_lpgm', None),
                output_data.get('magnitude', None), output_data.get('depth', None),
                output_data.get('sea', None),
                output_data.get('warning', None), output_data.get('tsunami_possible', None)
            ))
    elif table_name == 'VXSE51':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE51 (
                    max_int, report_time, quake_time, prefectures_en, prefectures_jp, regions_en, regions_jp
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('max_int', None), output_data.get('report_time', None),
                output_data.get('quake_time', None),
                ', '.join(output_data.get('prefectures_en', [])) if output_data.get('prefectures_en') else None,
                ', '.join(output_data.get('prefectures_jp', [])) if output_data.get('prefectures_jp') else None,
                ', '.join(output_data.get('regions_en', [])) if output_data.get('regions_en') else None,
                ', '.join(output_data.get('regions_jp', [])) if output_data.get('regions_jp') else None
            ))
    elif table_name == 'VXSE52':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VXSE52 (
                    epi_location_en, epi_location_jp, report_time, quake_time, magnitude, depth, no_tsunami
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                ', '.join(output_data.get('epi_location_en', [])) if output_data.get('epi_location_en') else None,
                ', '.join(output_data.get('epi_location_jp', [])) if output_data.get('epi_location_jp') else None,
                output_data.get('report_time', None), output_data.get('quake_time', None),
                output_data.get('magnitude', None), output_data.get('depth', None),
                output_data.get('no_tsunami', None)
            ))
    elif table_name == 'VTSE41':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VTSE41 (
                    report_time, quake_time, regions_en, regions_jp, heights, warning_level
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('report_time', None), output_data.get('quake_time', None),
                ', '.join(output_data.get('regions_en', [])) if output_data.get('regions_en') else None,
                ', '.join(output_data.get('regions_jp', [])) if output_data.get('regions_jp') else None,
                ', '.join(str(h) for h in output_data.get('heights', [])) if output_data.get('heights') else None,
                output_data.get('warning_level', None)
            ))
    elif table_name == 'VTSE51':
        with db.Database() as cursor:
            cursor.execute('''
                INSERT INTO VTSE51 (
                    report_time, quake_time, regions_en, regions_jp, heights, arrival_times, obs_regions_en, obs_regions_jp,
                    obs_heights, warning_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                output_data.get('report_time', None), output_data.get('quake_time', None),
                ', '.join(output_data.get('regions_en', [])) if output_data.get('regions_en') else None,
                ', '.join(output_data.get('regions_jp', [])) if output_data.get('regions_jp') else None,
                ', '.join(str(h) for h in output_data.get('heights', [])) if output_data.get('heights') else None,
                ', '.join(str(h) for h in output_data.get('arrival_times', [])) if output_data.get('arrival_times') else None,
                ', '.join(output_data.get('obs_regions_en', [])) if output_data.get('obs_regions_en') else None,
                ', '.join(output_data.get('obs_regions_jp', [])) if output_data.get('obs_regions_jp') else None,
                ', '.join(str(h) for h in output_data.get('obs_heights', [])) if output_data.get('obs_heights') else None,
                output_data.get('warning_level', None)
            ))
    else:
        dmdws.logger.warning('No table_name was matched, got:', table_name)



app = Flask('webquake') # Create a Flask app
sock = Sock(app)

open_sockets: set[WebsocketBase] = set() # Set of all open sockets

def send_data_to_all_sockets(data: dict):
    json_data = json.dumps(data, separators=(',', ':')) # Convert the data to a JSON string
    for ws in open_sockets.copy():
        threading.Thread(target=send_data, args=(ws, json_data), daemon=True).start() # Send the data to each open socket
        
def send_jst_time():
    while True:
        jst_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 9 * 3600)) # Get the current JST time
        send_data_to_all_sockets({'jst_time': jst_time})
        time.sleep(1) # Send the time every second

threading.Thread(target=send_jst_time, daemon=True).start() # Start the thread to send the JST time

recent_earthquake_data = None
recent_tsunami_data = None
recent_past_quake_data = None

def store_recent_data(output_data, data):
    global recent_earthquake_data, recent_tsunami_data
    # Only store earthquake data if "last_report" is present (VXSE43, VXSE45, VXSE47)
    if output_data['type'] == 'earthquake' and 'last_report' in output_data:
        recent_earthquake_data = output_data # Store the data
        dmdws.logger.info('Recent quake data stored')
        threading.Timer(120, clear_recent_earthquake_data).start() # Clear the data after 120 seconds
    elif output_data['type'] == 'tsunami':
        recent_tsunami_data = output_data
        dmdws.logger.info('Recent tsunami data stored')
        valid_time = int(time.mktime(time.strptime(data.get('body', {}).get('validDateTime', ''), "%Y-%m-%dT%H:%M:%S%z"))) # Get the valid time of the data
        current_time = int(time.time() + 9 * 3600)  # JST time
        expiry_secs = max(0, valid_time - current_time)
        threading.Timer(expiry_secs, clear_recent_tsunami_data).start()
    elif output_data['type'] == 'earthquake' and 'last_report' not in output_data:
        recent_past_quake_data = output_data
        dmdws.logger.info('Recent past quake data stored')
        threading.Timer(180, clear_recent_past_quake_data).start()
    else:
        dmdws.logger.info('Recent data not stored as it is not eligible')

def clear_recent_earthquake_data():
    global recent_earthquake_data
    recent_earthquake_data = None
    dmdws.logger.info('Recent quake data cleared')

def clear_recent_tsunami_data():
    global recent_tsunami_data
    recent_tsunami_data = None
    dmdws.logger.info('Recent tsunami data cleared')

def clear_recent_past_quake_data():
    global recent_past_quake_data
    recent_past_quake_data = None
    dmdws.logger.info('Recent past quake data cleared')

def send_recent_data(ws: WebsocketBase):
    if recent_earthquake_data:
        send_data(ws, json.dumps(recent_earthquake_data, separators=(',', ':')))
    if recent_tsunami_data:
        send_data(ws, json.dumps(recent_tsunami_data, separators=(',', ':')))
    if recent_past_quake_data:
        send_data(ws, json.dumps(recent_past_quake_data, separators=(',', ':')))

def send_alert(output_data):
    warning = output_data.get('warning', False) # Check if the data is a warning

    if warning == True:
        location_en = output_data.get('epi_location_en', 'Unknown')
        location_ja = output_data.get('epi_location_jp', 'Unknown')
        max_intensity = output_data.get('max_int', 'Unknown')
        current_time = time.time()
        if not hasattr(send_alert, 'last_warning_time') or current_time - send_alert.last_warning_time > 10: # Send the notification if it has been more than 10 seconds since the last one
            send_notification("warnings", "Earthquake Early Warning", f"Epicenter: {location_en} Max Int. {max_intensity}") # Send the notification in English
            send_notification("warnings_ja", "緊急地震速報", f"震源地: {location_ja} 最大震度 {max_intensity}") # Send the notification in Japanese
            send_alert.last_warning_time = current_time
            dmdws.logger.info('Earthquake warning notification sent')
        else:
            dmdws.logger.info('Earthquake warning notification not sent as it has been less than 10 seconds since the last one')

    elif output_data['type'] == 'earthquake' and warning == False:
        location_en = output_data.get('epi_location_en', 'Unknown')
        location_ja = output_data.get('epi_location_jp', 'Unknown')
        max_intensity = output_data.get('max_int', 'Unknown')
        current_time = time.time()
        if not hasattr(send_alert, 'last_forecast_time') or current_time - send_alert.last_forecast_time > 10:
            send_notification("warnings", "Earthquake Forecast", f"Epicenter: {location_en} Max Int. {max_intensity}")
            send_notification("warnings", "地震動予測", f"震源地: {location_ja} 最大震度 {max_intensity}")
            send_alert.last_forecast_time = current_time
            dmdws.logger.info('Earthquake forecast notification sent')
        else:
            dmdws.logger.info('Earthquake forecast notification not sent as it has been less than 10 seconds since the last one')
            
    elif output_data['type'] == 'tsunami': # Tsunami notifications are always sent
            send_notification("warnings", "Tsunami Information", f"New Tsunami Information issued. Open WebQuake for more details.")
            send_notification("warnings_ja", "津波情報", f"新たな津波情報が発表された。詳細はWebQuakeをご覧ください。")
            dmdws.logger.info('Tsunami information notification sent')

    else:
        dmdws.logger.info('No notification sent as data type does not call for it')

def send_data(ws: WebsocketBase, data: str):
    if ws.connected:
        try: # Try to send the data
            ws.send(data)
            return
        except:
            pass # If the send fails, the socket is likely closed
    
    open_sockets.remove(ws) # Remove the socket from the set if it is closed
    
 #   def console_send_data():
  #      while True:
   #         command = input("Type 'send' to send data through the websocket: ")
    #        if command.lower() == 'send':
     #           data = input("Enter the data to send: ")
      #          send_data_to_all_sockets(json.loads(data))
#
 #   threading.Thread(target=console_send_data, daemon=True).start()

@sock.route('/ws')
def websocket_recv_open(ws: WebsocketBase):
    open_sockets.add(ws) # Add the socket to the set of open sockets
    send_recent_data(ws) # Send the recent data to the new socket

    while True:
        message = ws.receive(timeout=12) # If ping not recieved every 12 seconds, close the connection
        if message is None:
            break

        if message == '!ping': # Respond to a ping
            ws.send('!pong')
        
        if ws not in open_sockets:
            break

    try:
        ws.send('!timeout') # Send a timeout message before closing the connection
    except:
        pass

    try:
        open_sockets.remove(ws) # Remove the socket from the set of open sockets
    except:
        pass


def send_notification(category_tag, title, message):
    app_id = "9e4a84de-c3f9-4e84-b285-f1601c5e37de"
    rest_api_key = "os_v2_app_tzfijxwd7fhijmuf6fqbyxrx32j4fmzmwgbeip5wbswtq57tzi4snplrul5eu6jpigodx7zheewfdfqdoqjg4uppnbg2bu634gwtxxq"

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {rest_api_key}",
    }

    # Target users with the specified tag
    filters = [
        {"field": "tag", "key": category_tag, "relation": "=", "value": "subscribed"},
    ]

    payload = {
        "app_id": app_id,
        "filters": filters,
        "headings": {"en": title},
        "contents": {"en": message},
    }

    url = "https://onesignal.com/api/v1/notifications"

    response = requests.post(url, headers=headers, json=payload)

def listen_for_notif():
    while True:
        cmd = input("Type 'notif' to send quake alert: ")
        if cmd.lower() == 'notif':
            send_notification("warnings", "Earthquake Alert", "TEST NOTIFICATION")
            time.sleep(1)
            send_notification("warnings_ja", "地震警報", "これはテストだ")

threading.Thread(target=listen_for_notif, daemon=True).start()
# Setting the message listener (tells dmdws what function to call when a message is received) 
conn.set_message_listener(on_message, threading=False)

# Starting the connection, "run" will block the program indefinitely.
conn.start()

app.run('0.0.0.0', 8000) # Start the Flask app on port 8000
listen_for_notif()
#on_message(conn._attempt_decode(json.load(open('9.json' , 'r', encoding='utf-8'))))