#For every station: find modal stopping time between stopovers.
#3 columns: from, to, stopover, stopovertime

import psycopg2
import datetime
import re
from operator import itemgetter 

#establish database connection
to = datetime.datetime.now()
try:
    conn = psycopg2.connect("dbname='fswork' user='fsuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#load timetable and frequencies

tt = open("timetbls.dat")
ft = open("footnote.dat")

# create dictionary of train routes
routedict = {}
for line in tt:
    if line.startswith("#"):
        routelist = []
        keyname = line[0:9]
    if line.startswith(">") or line.startswith("+") or line.startswith("."): #start en stops
        lineholder = re.findall(r'\w+', line)
        
    #corrigeren voor nachttreinen
        if int(lineholder[1]) > 2359 and int(lineholder[1]) <= 2459:
            lineholder[1] = "00%d" % (int(lineholder[1]) - 2400)
        if int(lineholder[1]) > 2459:
            lineholder[1] = "0%d" % (int(lineholder[1]) - 2400)
        if len(lineholder)>2:
            if int(lineholder[2]) > 2359 and int(lineholder[2]) <= 2459:
                lineholder[2] = "00%d" % (int(lineholder[2]) - 2400)
            if int(lineholder[2]) > 2459:
                lineholder[2] = "0%d" % (int(lineholder[2]) - 2400)
    #converteer naar tijden
        lineholder[1] = datetime.datetime.strptime(lineholder[1], '%H%M').time()
        if len(lineholder)>2:
            lineholder[2] = datetime.datetime.strptime(lineholder[2], '%H%M').time()
        routelist.append(lineholder)

    if line.startswith("<"): #eindpunt 
        lineholder = re.findall(r'\w+', line)

    #corrigeren voor nachttreinen
        if int(lineholder[1]) > 2359:
            lineholder[1] = "0%d" % (int(lineholder[1]) - 2400)        
        lineholder[1] = datetime.datetime.strptime(lineholder[1], '%H%M').time()
        routelist.append(lineholder)

    if line.startswith('-'):
        routelist.append(line[1:6])
    
    # add to dictionary
        routedict[keyname] = routelist

te = datetime.datetime.now()
print("Timetable parsed")
print("Time spent: %s \n" % (te-to))

# parse footnotes. Find out which trains go from mon-fri
# defitinion file says: runs from sunday 10-12-2017 to monday 08-12-2018 
rosterlist = []
for line in ft:
    if line.startswith("#"):
        keyname = line[0:6]
    elif line.startswith("0") or line.startswith("1"):
        roster = line[0:364]
        freq = roster.count('1')
        rosterlist.append([keyname, roster, freq])

#change footnote id to frequency over year
for k, v in routedict.items():
    for rosteritem in rosterlist:
        if rosteritem[0][1:] == v[0]:
            v[0] = rosteritem[2]

to = datetime.datetime.now()
print("Footnotes parsed")
print("Time spent: %s \n" % (to-te))

#download stations from dbase
loadstations = """SELECT * FROM stations"""

cur.execute(loadstations)
stationstuple = cur.fetchall()
stations = []
for row in stationstuple:
    stations.append(list(row))

#download valid routes from dbase
loadvalidtimes = """SELECT * FROM traveltimes WHERE firsttimeto IS NOT NULL"""

cur.execute(loadvalidtimes)
validtraveltuple = cur.fetchall()
validtravel = []
validtraveltimes = {}
for row in validtraveltuple:
    validtravel.append([row[1],row[2]])

#loop through all stations
for station in stations[0:2]:
    stopovertime = int(station[-2])
    print(station)
    #create dictionary with only routes going through the station
    stationdict = {}
    for k, v in routedict.items():
        for w in v:
            if isinstance(w, int): #if first line from dict value (= frequency of train)
                freq = w
            elif isinstance(w, list) and w[0] == station[2]:
                stationdict[freq] = v[1:]

    #loop through stationdict and find stations one can get fro and one can get to
    arrivals = []
    departures = []
    today = datetime.date.today()
    for k, v in stationdict.items():
        for w in v:
            if w[0] == station[2]:
                arrivaltime = w[1]
                departuretime = w[-1]
                break

        for w in v:
            if datetime.datetime.combine(today, w[1]) < datetime.datetime.combine(today, arrivaltime):
                arrivals.append([w[0], arrivaltime, k])
            elif datetime.datetime.combine(today, w[-1]) > datetime.datetime.combine(today,departuretime):
                departures.append([w[0], departuretime, k])

    #find combinations of all departure and arrival stations
    arrivalstations = [x[0] for x in arrivals]
    departurestations = [x[0] for x in departures]
    connections = [[x,y] for x in arrivalstations for y in departurestations]
    
    #for every connection, calculate modal short stopover time
    for connection in connections:
        
        #create narrow set of arrivals and departures and sort
        arrivalset = []
        departureset = []
        for arrival in arrivals:
            if arrival[0] == connection[0]:
                arrivalset.append(arrival)
        arrivalset.sort(key=itemgetter(1))
        for departure in departures:
            if departure[0] == connection[1]:
                departureset.append(departure)
        departureset.sort(key=itemgetter(1))

        #for every in arrivalset, search nearest departure
        comboset = []
        today = datetime.date.today()
        for arrival in arrivalset:
            arrivaltime = datetime.datetime.combine(today, arrival[1])
            for departure in departureset:
                departuretime = datetime.datetime.combine(today, departure[-2])
                if departuretime > (arrivaltime + datetime.timedelta(minutes=(stopovertime+30))):
                    stopovertime = round((datetime.datetime.combine(today, departure[-2]) - datetime.datetime.combine(today, arrival[1])).total_seconds()/60)
                    comboset.append([arrival, departure, stopovertime])
                    break

        for row in comboset:
            print(row)


    departures = []
    arrivals = []
    stopoverstation = station[2]
    for trip in validtravel:
        if stopoverstation == trip[1]:
            arrivals.append(trip[0])
        elif stopoverstation == trip[0]:
            departures.append(trip[1])
    print(len(arrivals))
    print(len(departures))
    connections = [[x,y] for x in arrivals for y in departures]

#WORK WITH ROUTE DICT FROM TIMEREAD: ONLY OF COLLECTION OF TRAINS
"""