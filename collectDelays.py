# RMV Open Data API
# https://opendata.rmv.de/site/start.html
# Get Client ID and API key from the Developer Console

import settings
import urllib.request
import json
from pymongo import MongoClient

client = MongoClient(settings.CONNECTION_STRING) # get MongoDB Client

db = client['RMV-Pull-Stop']                     # create mono db if not exists

collection = db["GG-FFM"]                        # create collection if not exists
#collection.drop()                               

# get pull-stop data from RMV API
def pull_stop_data(stopID: str, direction: str): 
    source_url = f"https://www.rmv.de/hapi/departureBoard?id={stopID}&direction={direction}&accessId={settings.API_TOKEN}&format=json"
    with urllib.request.urlopen(source_url) as url:
        data: str = json.loads(url.read().decode())

    # write to file
    f = open("data/departureBoard.json", "w")
    f.write(json.dumps(data, indent = 4, sort_keys=True))
    f.close()
    #print(json.dumps(data, indent = 4, sort_keys=True))
    
    stopData = []

    for departure in data["Departure"]:         # collect some data
        dictionary = {}
        dictionary['stop'] = departure['stop']
        dictionary['direction'] = departure['direction']
        dictionary['date'] = departure['date']
        dictionary['time'] = departure['time']
        if 'rtDate' in departure:               # date of delayed arrival
            dictionary['rtDate'] = departure['rtDate']
        if 'rtTime' in departure:               # time of delayed arrival
            dictionary['rtTime'] = departure['rtTime']
        dictionary['name'] = departure['name']
        # Belegung
        raw = departure['Occupancy'][0]['raw']
        key = "text.occup.jny.max." + str(raw)
        for n in departure['Notes']['Note']:
            if n['key'] == key:
                print(n['value'])
                dictionary['occupancy'] = n['value']
        stopData.append(dictionary)
    return stopData


# Save data in mongo db
for direction in settings.directions: # get some departure data per direction
    pullStopDir = pull_stop_data(settings.stopID, direction)
    for psd in pullStopDir:
        # insert departure data in document if departure times not exist
        collection.update_one(  
            {
                'date' : psd.get("date"), 
                'time' : psd.get("time")
            },
            {
                '$setOnInsert': psd
            },
                upsert = True
        )
        # update delay data in existing document if changed
        collection.update_one(  
            {
                'date' : psd.get("date"), 
                'time' : psd.get("time")
            }, 
            {
                '$set': { 
                    'rtDate': psd.get("rtDate"), 
                    'rtTime': psd.get("rtTime")
                }
            },
            upsert = True
        )
# print all data of collection
query = {  }  
doc = collection.find(query)
for d in doc:
  print(d)
