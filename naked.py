import logging
import logging.config
import requests
import json
import datetime #importē nepieciešāmās bibliotēkas.
import time
import yaml

from datetime import datetime
from configparser import ConfigParser

# Loading logging configuration
with open('./log_worker.yaml', 'r') as stream:
	 log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

# Creating logger
logger = logging.getLogger('root')

logger.info('Asteroid processing service')

# Initiating and reading config values
logger.info('Loading configuration from file')

try:
	config = ConfigParser()
	config.read('config.ini')

	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')

except:
	logger.exception('')
logger.info('DONE')


# Getting todays date
dt = datetime.now()  #Atgriež patreizējo datumu un laiku un piešķir to mainīgajam "dt"
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2) #Izveido mainīgo kurā glabājas gads, mēnesis un diena kurus iegūst izvelkot no mainīgā "dt". 
logger.debug("Generated today's date: " + str(request_date)) #Izprintē šodien uzģenerēto datumu izsaucot mainīgo "request_date".


logger.debug("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)) #Izdrukā sakombinētu url kas sastāv no nasa api url, request date mainīgā un nasa api key.
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key) #Izveido mainīgo r, kas iegūst datus no kombinētā url.

logger.debug("Response status code: " + str(r.status_code)) #Izdrukā statusa kodu, kas parāda vai status ir ok (200) vai is not found (404).
logger.debug("Response headers: " + str(r.headers)) #Izdrukā vai ir izdevusies autentifikacija.
logger.debug("Response content: " + str(r.text)) #Izdrukā unikodā sniegtās atbildes.

if r.status_code == 200: #Kods turpināsies ja r.statusa kods būs 200.

	json_data = json.loads(r.text) #Parsē iegūtos json datus lai pārvērstu tos python saprotamā vārdnīcā.

	ast_safe = [] #Izveido masīvu ar drošajiem asteroīdiem.
	ast_hazardous = [] #izveido masīvu ar nedrošajiem asteroīdiem.

	if 'element_count' in json_data: #Pārbauda vai json datos ir atslēga "element_count", ja tā ir tad tiks izpildīts koda bloks.
		ast_count = int(json_data['element_count']) #Izveido mainīgo ast_count kam piešķir vērtību element_count.
		logger.info("Asteroid count today: " + str(ast_count)) #Izdrukā asteroīdu skaitu cik tie šodien ir.

		if ast_count > 0: #Pārbauda vai asteroīdu skaits ir lielāks par 0.
			for val in json_data['near_earth_objects'][request_date]: #Cikls kurš izmanto json datu atslēgas "near_earth_objects" vērtības konkrētam pieprasījuma datumam.
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val: #Parbauda, vai "val" ir pieejamas atslēgas (name, nasa_jpl_url, estimated_diameter, is_potentially_hazardous_asteroid, close_aproach_data), ja ir visas šīs atslēgas tad tiks izpildīts koda bloks.
					tmp_ast_name = val['name'] #Izveido mainīgo kam tiek piešķirta vērtība no val['name'].
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url'] #Izveido mainīgo kam tiek piešķirta vērtība val['nasa_jpl_url'].
					if 'kilometers' in val['estimated_diameter']: #Pārbauda vai  "kilometers" ir pieejams iekš val['estimated_diameter].
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']: #Pārbauda vai "estimated_diameter_min" un "estimated_diameter_max" ir pieejams iekš val['estimated_diameter']							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3) #Izveido mainīgo, kur
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3) #Izveido manīgo kurā tiek aprēķināts un noapaļots asteroīda diametrs
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)

						else: #Izpildās ja "if" nenostrādā
							tmp_ast_diam_min = -2 #Izveidots mainīgais un tam piešķirta vērtība -2.
							tmp_ast_diam_max = -2 #Izveidots mainīgais un tam piešķirta vērtība -2.
					else: #Izpildās ja "if" nenostrādā
						tmp_ast_diam_min = -1 #Izveidots mainīgais un tam piešķirta vērtība -2.
						tmp_ast_diam_max = -1 #Izveidots mainīgais un tam piešķirta vērtība -2.

					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid'] #Izveidots jauns mainīgais kuram tiek iedota vērtība "is_potentially_hazardous_asteroid".

					if len(val['close_approach_data']) > 0: #Pārbauda vai asteroīdu close_approach_data ir lielāks par 0.
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]: #Pārbauda vai ('epoch_date_close_approach' and 'relative_velocity' and 'miss_distance') ir atrodami iekš val['close_approach_data'].
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000) #Izveidots mainīgais kurā tiek pārveidots asteroīda tuvošanās laika brīdis no milisekundēm uz sekundēm.
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S') #Izveidots mainīgais kurā pārveido timestamp vērtību uz utc laiku.
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S') #Izveidots mainīgais pārveido timestamp patreizēja laikā

							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']: #Pārbauda vai "kilometers_per_hours" eksistē iekš val['close_approach_data'].
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour'])) #Izveido mainīgo kurā tiek  pārveidots asteroīda ātrums flaot uz intigeri.
							else: #Izpildās ja "if" nenostrādā
								tmp_ast_speed = -1 #Izveidots mainīgais ar asteroīda ātrumu -1.

							if 'kilometers' in val['close_approach_data'][0]['miss_distance']: #Pārbauda vai "kilometers" eksistē iekš val['close_approach_data'].
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3) #Izveidots mainīgais kurā tiek noapaļots miss distanec kas tiek mērīta kilometros un pārvērsta float datu tipā.
							else: #Izpildās ja "if" nenostrādā
								tmp_ast_miss_dist = -1 #Izveidots mainīgais miss distance ar vērtību -1.
						else: #Izpildās ja "if" nenostrādā
							tmp_ast_close_appr_ts = -1 #Izveidots mainīgais ar vērtību -1.
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59" #Izveidots mainīgais ar default vērtībām.
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"	#Izveidots mainīgais ar default vērtībām.
					else: #Izpildās ja "if" nenostrādā
						logger.warning("No close approach data in message") #Ja nav bijis tuvs asteroīds tad tiek izvadīts šis paziņojums.
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1 #Ja nav bijis tuvs asteroīds tad tiek uzstādītas default vērtības.

					logger.info("------------------------------------------------------- >>") #Izdrukā līniju.
					logger.info("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous)) #Izdrukā informāciju par asteroīdu.
					logger.info("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt)) #Izdrukā laiku kad asteroīds būs tuvu. 
					logger.info("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km") #Izdruka asteroīda ātrumu un miss distanci.

					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True: #Pārbauda vai asteroīds ir bīstams.
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist]) #Pievieno asteorīda datus bīstamo asteroīdu masīvam 
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist]) #Pievieno asteroīda datus drošajiem.

		else:
			logger.info("No asteroids are going to hit earth today")

	logger.info("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe))) #Izprintē nedrošo asteroīdu skaitu un drošo asteroīdu skaitu.

	if len(ast_hazardous) > 0: #Pārbauda vai masīvā ir nedrošie asteroīdi.

		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		logger.info("Today's possible apocalypse (asteroid impact on earth) times:")
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		logger.info("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		logger.info("No asteroids close passing earth today")

else:
	logger.error("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
