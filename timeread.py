import re
import datetime
import csv
from operator import itemgetter

to = datetime.datetime.now()

tt = open("timetbls.dat")
ft = open("footnote.dat")

# create dictionary of train routes
routedict = {}
for line in tt:
    if line.startswith("#"):
        routelist = []
        keyname = line[0:9]
    if line.startswith(">") or line.startswith("+"):
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

    if line.startswith("<"):
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

#parse stations
with open('stations.csv', newline='') as csvfile:
    stationtable = list(list(rec) for rec in csv.reader(csvfile, delimiter=',', quotechar='"'))

with open('stationstimes.csv', newline='') as csvfile:
    timestable = list(list(rec) for rec in csv.reader(csvfile, delimiter=',', quotechar='"'))

for s in stationtable:
    s[1] = s[1].lower()
    for t in timestable:
        if s[1] == t[1]:
            s.extend([t[2],t[0]])

# generate lists of departure and arrival stations
departures = []
arrivals = []
for line in stationtable:
    departures.append(line[1])
    arrivals.append(line[1])

te = datetime.datetime.now()
print("Stations parsed")
print("Time spent: %s \n" % (te-to))

#link up stations

station1 = ['Groningen', 'gn', '53.21055603', '6.564722061', 'Netherlands', '05', '1']
station2 = ['Amsterdam Centraal', 'asd', '52.37888718', '4.900277615', 'Netherlands', '05', '1']

#find times for station1 to station2
listofdep = []
for k, v in routedict.items():
    for dep in v:
        if isinstance(dep, int): #if first line from dict value (= frequency of train)
            freq = dep
        elif isinstance(dep, list) and dep[0] == station1[1]: #if list of train stops
            lookfrom = v.index(dep)
            deptime = dep[-1]
            connectiondummy = 0
            for arr in v[lookfrom:]:
                if arr[0] == station2[1]: #if combination is made: save dept, arr, difference
                    arrtime = arr[1]
                    td2 = datetime.date.today() #to handle trains leaving around 12am
                    if round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td2, deptime)).total_seconds()/60) < 0:
                        td1 = datetime.date.today() - datetime.timedelta(days=1)
                    else:
                        td1 = datetime.date.today()
                    difference = round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td1, deptime)).total_seconds()/60)
                    listofdep.append([k,freq,deptime, arrtime,difference])
                    connectiondummy = 1
            #if connectiondummy == 0:



#find times for station2 to station1
listofarr = []
for k, v in routedict.items():
    for dep in v:
        if isinstance(dep, int): #if first line from dict value (= frequency of train)
            freq = dep
        elif isinstance(dep, list) and dep[0] == station2[1]: #if list of train stops
            lookfrom = v.index(dep)
            deptime = dep[-1]
            for arr in v[lookfrom:]:
                if arr[0] == station1[1]: #if combination is made: save dept, arr, difference
                    arrtime = arr[1]
                    td2 = datetime.date.today() #to handle trains leaving around 12am
                    if round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td2, deptime)).total_seconds()/60) < 0:
                        td1 = datetime.date.today() - datetime.timedelta(days=1)
                    else:
                        td1 = datetime.date.today()
                    difference = round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td1, deptime)).total_seconds()/60)
                    listofarr.append([k,freq,deptime, arrtime,difference])

#for station1 to station2: calculate set plus frequency by time
listofdep.sort(key=itemgetter(4))

setofdep = []
for train in listofdep:
    if train == listofdep[0]: #first row
        frequencycontainer = train[1]
        totalfreqcontainer = train[1]
        differencecontainer = train[4]
    elif train != listofdep[0] and train[4] == differencecontainer: #if same time
        frequencycontainer += train[1]
        totalfreqcontainer += train[1]
    elif train != listofdep[0] and train[4] != differencecontainer: #if different time
        setofdep.append([differencecontainer, frequencycontainer])
        frequencycontainer = train[1]
        differencecontainer = train[4]
        totalfreqcontainer += train[1]
    elif train == listofdep[-1] and train[4] == differencecontainer: #last row, same time  
        frequencycontainer += train[1]
        setofdep.append([differencecontainer, frequencycontainer])
        totalfreqcontainer += train[1]
    elif train == listofdep[-1] and train[4] != differencecontainer: # last row, different time
        frequencycontainer = train[1]
        differencecontainer = train[4]
        setofdep.append([differencecontainer, frequencycontainer])
        totalfreqcontainer += train[1]

#for station 1 to station 2: find 3 most common travel times
setofdep.sort(key=itemgetter(1))

traveltime121 = setofdep[-1]
traveltime122 = setofdep[-2]
traveltime123 = setofdep[-3]

trainsperday12 = round(totalfreqcontainer/364)

#for station2 to station1: calculate set plus frequency by time
listofarr.sort(key=itemgetter(4))

setofarr = []
for train in listofarr:
    if train == listofarr[0]: #first row
        frequencycontainer = train[1]
        totalfreqcontainer = train[1]
        differencecontainer = train[4]
    elif train != listofarr[0] and train[4] == differencecontainer: #if same time
        frequencycontainer += train[1]
        totalfreqcontainer += train[1]
    elif train != listofarr[0] and train[4] != differencecontainer: #if different time
        setofarr.append([differencecontainer, frequencycontainer])
        frequencycontainer = train[1]
        differencecontainer = train[4]
        totalfreqcontainer += train[1]
    elif train == listofarr[-1] and train[4] == differencecontainer: #last row, same time  
        frequencycontainer += train[1]
        setofarr.append([differencecontainer, frequencycontainer])
        totalfreqcontainer += train[1]
    elif train == listofarr[-1] and train[4] != differencecontainer: # last row, different time
        frequencycontainer = train[1]
        differencecontainer = train[4]
        setofarr.append([differencecontainer, frequencycontainer])
        totalfreqcontainer += train[1]
 
#for station 2 to station 1: find 3 most common travel times
setofarr.sort(key=itemgetter(1))

traveltime211 = setofdep[-1]
traveltime212 = setofdep[-2]
traveltime213 = setofdep[-3]

trainsperday21 = round(totalfreqcontainer/364)

#find first and last travel times based on largest difference between departure times
listofdep.sort(key=itemgetter(2))
listofarr.sort(key=itemgetter(2))

for dep in listofdep:
    if dep == listofdep[0]:
        td2 = datetime.date.today() #to handle trains leaving around 12am
        if round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td2, listofdep[-1][2])).total_seconds()/60) < 0:
            td1 = datetime.date.today() - datetime.timedelta(days=1)
        else:
            td1 = datetime.date.today()
        difference = round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td1, listofdep[-1][2])).total_seconds()/60)
        dep.append(difference)
        lastdep = dep[2]
    else:
        td2 = datetime.date.today() #to handle trains leaving around 12am
        if round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td2, lastdep)).total_seconds()/60) < 0:
            td1 = datetime.date.today() - datetime.timedelta(days=1)
        else:
            td1 = datetime.date.today()
        difference = round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td1, lastdep)).total_seconds()/60)
        dep.append(difference)
        lastdep = dep[2]

for dep in listofarr:
    if dep == listofdep[0]:
        td2 = datetime.date.today() #to handle trains leaving around 12am
        if round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td2, listofdep[-1][2])).total_seconds()/60) < 0:
            td1 = datetime.date.today() - datetime.timedelta(days=1)
        else:
            td1 = datetime.date.today()
        difference = round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td1, listofdep[-1][2])).total_seconds()/60)
        dep.append(difference)
        lastdep = dep[2]
    else:
        td2 = datetime.date.today() #to handle trains leaving around 12am
        if round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td2, lastdep)).total_seconds()/60) < 0:
            td1 = datetime.date.today() - datetime.timedelta(days=1)
        else:
            td1 = datetime.date.today()
        difference = round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td1, lastdep)).total_seconds()/60)
        dep.append(difference)
        lastdep = dep[2]

firsttraveltime12 = max(listofdep, key=lambda x: x[5])[2]
firsttraveltime21 = max(listofarr, key=lambda x: x[5])[2]

lasttraveltime12 = listofarr[(listofarr.index(max(listofarr, key=lambda x: x[5]))-1)][2]
lasttraveltime21 = listofdep[(listofdep.index(max(listofdep, key=lambda x: x[5]))-1)][2]

#append to list
travellist = []
travellist.append([
    station1, 
    station2, 
    traveltime121[0],
    traveltime121[1],
    traveltime122[0],
    traveltime122[1],
    traveltime123[0],
    traveltime123[1],
    trainsperday12,
    firsttraveltime12,
    lasttraveltime12
])

travellist.append([
    station2, 
    station1, 
    traveltime211[0],
    traveltime211[1],
    traveltime212[0],
    traveltime212[1],
    traveltime213[0],
    traveltime213[1],
    trainsperday21,
    firsttraveltime21,
    lasttraveltime21
])

for row in travellist:
    print(row)

# For every approach: loop through every trainroute
# Append every track that go to destination.
# First collect all the direct train connections
# Then create new list of all nondirect connections.
# sync up based on switchover times. 
# Then another list etc etc.


"""

end product is list of lists

first: stopovers!!

generalize on a per day basis. Calculate travel time: mode
number of times a day

# find out what trains go on what day
calendardict = {}
for day in range(0,364):
    footnotelist = []
    for k, v in rosterdict.items():
            if v[day] == "1":
                footnotelist.append(k)
    calendardict[day] = footnotelist

                
spread = []



"""