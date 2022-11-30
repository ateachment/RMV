# RMV Open Data API
# https://opendata.rmv.de/site/start.html
# Get Client ID and API key from the Developer Console

import settings
import urllib.request
import json
import os
from pymongo import MongoClient
from datetime import datetime
import csv

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
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, "data/departureBoard.json")
    f = open(filename, "w")
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
        dictionary['name'] = departure['name']
        if 'rtDate' in departure:               # date of delayed arrival
            dictionary['rtDate'] = departure['rtDate']
        if 'rtTime' in departure:               # time of delayed arrival
            dictionary['rtTime'] = departure['rtTime']
        
        if 'Occupancy' in departure:  # if info 'Belegung' available 
            raw = departure['Occupancy'][0]['raw']
            key = "text.occup.jny.max." + str(raw)
            for n in departure['Notes']['Note']:
                if n['key'] == key:
                    #print(n['value'])
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
query = {'date':{'$exists': True} }  
doc = collection.find(query)
for d in doc:
  print(d)

# write to csv file
f = open('data/delays.csv', 'w', newline='')
writer = csv.writer(f)
fields = ['datetime', 'delay', 'name', 'occupancy']
writer.writerow(fields)

query = { }  
doc = collection.find(query,{'_id':False}) # exlude _id from result dict (save memory)
for d in doc:
    datetimePlanned = datetime.strptime(d['date']+d['time'],"%Y-%m-%d%H:%M:%S")
    if 'rtDate' not in d:
        d['rtDate'] = d['date']
    if d['rtDate'] == None:
        d['rtDate'] = d['date']
    if d['rtTime'] == None:
        d['rtTime'] = d['time']
    if 'name' not in d:
        d['name'] = d['product']
    if 'occupancy' not in d:
        d['occupancy'] = "-"
    datetimeDelayed = datetime.strptime(d['rtDate']+d['rtTime'],"%Y-%m-%d%H:%M:%S")
    delay = datetimeDelayed - datetimePlanned
    print(datetimeDelayed.isoformat() + " - " + datetimePlanned.isoformat() + " = " + str(delay))
    data = [datetimePlanned.isoformat(),str(delay), d['name'], d['occupancy']]
    writer.writerow(data)

f.close()
