import re
import datetime
import csv

from operator import itemgetter
import itertools

to = datetime.datetime.now()

tt = open("timetbls.dat")
ft = open("footnote.dat")

# create dictionary of train routes
routedict = {}
for line in tt:
    if line.startswith("#"):
        routelist = []
        keyname = line[0:9]
    if line.startswith(">") or line.startswith("+"): #start en stops
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
for line in stationtable:
    departures.append(line[1])

stationpairs = [] 
for subset in itertools.combinations(departures, 2):
    stationpairs.append(subset)   

te = datetime.datetime.now()
print("Stations parsed")
print("Time spent: %s \n" % (te-to))

#link up stations

#find times for station1 to station2
alldepslist = []
traveltimeslist = []
for pair in stationpairs:
    listofdep =[]
    
    for k, v in routedict.items():
        for dep in v:
            if isinstance(dep, int): #if first line from dict value (= frequency of train)
                freq = dep
            elif isinstance(dep, list) and dep[0] == pair[0]: #if list of train stops
                lookfrom = v.index(dep)
                deptime = dep[-1]
                
                for arr in v[lookfrom:]: # CHECKS FOR DIRECT CONNECTIONS
                    if arr[0] == pair[1]: #if combination is made: save dept, arr, difference
                        arrtime = arr[1]
                        td2 = datetime.date.today() #to handle trains leaving around 12am
                        if round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td2, deptime)).total_seconds()/60) < 0:
                            td1 = datetime.date.today() - datetime.timedelta(days=1)
                        else:
                            td1 = datetime.date.today()
                        difference = round((datetime.datetime.combine(td2, arrtime) - datetime.datetime.combine(td1, deptime)).total_seconds()/60)
                        listofdep.append([k,freq,deptime, arrtime,difference])
    
    #find times for station2 to station1
    listofarr = []
    for k, v in routedict.items():
        for dep in v:
            if isinstance(dep, int): #if first line from dict value (= frequency of train)
                freq = dep
            elif isinstance(dep, list) and dep[0] == pair[1]: #if list of train stops
                lookfrom = v.index(dep)
                deptime = dep[-1]
                
                for arr in v[lookfrom:]: # CHECKS FOR DIRECT CONNECTIONS
                    if arr[0] == pair[0]: #if combination is made: save dept, arr, difference
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
    totalfreqcontainer = 0
    
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
    try:
        setofdep.append([differencecontainer, frequencycontainer])   
    except:
        pass
    #for station 1 to station 2: find 3 most common travel times
    setofdep.sort(key=itemgetter(1))

    try:
        traveltime121 = setofdep[-1]
    except:
        traveltime121 = [None, None]

    try:
        traveltime122 = setofdep[-2]
    except:
        traveltime122 = [None, None]

    try:
        traveltime123 = setofdep[-3]
    except:
        traveltime123 = [None, None]
    
    trainsperday12 = round(totalfreqcontainer/364)

    #for station2 to station1: calculate set plus frequency by time
    listofarr.sort(key=itemgetter(4))
    
    setofarr = []
    totalfreqcontainer = 0
    
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
    try:
        setofarr.append([differencecontainer, frequencycontainer])
    except:
        pass
    #for station 2 to station 1: find 3 most common travel times
    setofarr.sort(key=itemgetter(1))

    try:
        traveltime211 = setofarr[-1]
    except:
        traveltime211 = [None, None]

    try:
        traveltime212 = setofarr[-2]
    except:
        traveltime212 = [None, None]

    try:
        traveltime213 = setofarr[-3]
    except:
        traveltime213 = [None, None]
    
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
        if dep == listofarr[0]:
            td2 = datetime.date.today() #to handle trains leaving around 12am
            if round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td2, listofarr[-1][2])).total_seconds()/60) < 0:
                td1 = datetime.date.today() - datetime.timedelta(days=1)
            else:
                td1 = datetime.date.today()
            difference = round((datetime.datetime.combine(td2, dep[2]) - datetime.datetime.combine(td1, listofarr[-1][2])).total_seconds()/60)
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

    if len(listofdep) > 2:
        firsttraveltime12 = max(listofdep, key=lambda x: x[5])[2]
        if listofdep[(listofdep.index(max(listofdep, key=lambda x: x[5]))-1)][2]:
            lasttraveltime21 = listofdep[(listofdep.index(max(listofdep, key=lambda x: x[5]))-1)][2]
        else:
            lasttraveltime21 = min(listofdep, key=lambda x: x[5])[2]
    else:
        firsttraveltime12 = None
        lasttraveltime21 = None        
    
    if len(listofarr) > 2:
        firsttraveltime21 = max(listofarr, key=lambda x: x[5])[2]
        if listofarr[(listofarr.index(max(listofarr, key=lambda x: x[5]))-1)][2]:
            lasttraveltime12 = listofarr[(listofarr.index(max(listofarr, key=lambda x: x[5]))-1)][2]
        else:
            lasttraveltime12 = min(listofarr, key=lambda x: x[5])[2]
    else:
        firsttraveltime21 = None
        lasttraveltime12 = None

    #append to list
    print([
        pair[0], 
        pair[1], 
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

    print([
        pair[1], 
        pair[0], 
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