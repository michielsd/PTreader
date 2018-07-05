
import psycopg2
import datetime
import itertools
from operator import itemgetter

#function to convert postgres time to datetime time
def trainimporter(depstation, arrstation):
    loadtrains = """SELECT depstation, arrstation, train, frequency, deptime, arrtime, difference FROM trains WHERE frequency > 80 AND depstation='%s' AND arrstation='%s'""" % (depstation, arrstation)
    cur.execute(loadtrains)
    trainstuple = cur.fetchall()
    trains = []
    for row in trainstuple:
        trains.append(list(row))

    trains.sort(key=itemgetter(3))
    if len(trains) > 100:
        trainsredux = trains[-100:]
    else:
        trainsredux = trains

    return trainsredux

def stopimporter(depstation, arrstation, arrtime,stoptime):
    today = datetime.date.today()
    hours = int(arrtime[0:2])
    minutes = int(arrtime[3:5])
    seconds = int(arrtime[6:8])  

    firstdeptime = datetime.time(hours, minutes, seconds)
    workingtime = datetime.datetime.combine(today, firstdeptime)
    lastdeptime = (workingtime + datetime.timedelta(minutes=int(stoptime)) + datetime.timedelta(minutes=30)).time()
    
    loadtrains = """SELECT depstation, arrstation, train, frequency, deptime, arrtime, difference FROM trains WHERE frequency > 80 AND depstation='%s' AND arrstation='%s' AND deptime BETWEEN '%s' AND '%s'""" % (depstation, arrstation, firstdeptime, lastdeptime)
    cur.execute(loadtrains)
    trainstuple = cur.fetchall()
    trains = []
    for row in trainstuple:
        trains.append(list(row))

    trains.sort(key=itemgetter(3))
    if len(trains) > 100:
        trainsredux = trains[-100:]
    else:
        trainsredux = trains

    return trainsredux    

def modefinder(departuretime, stoparrival, depfrequency, triplist):
    today = datetime.date.today()
    
    hours = int(departuretime[0:2])
    minutes = int(departuretime[3:5])
    seconds = int(departuretime[6:8])
    deptime = datetime.time(hours, minutes, seconds)

    checklist = []
    for trip in triplist:
        hours = int(trip[5][0:2])
        minutes = int(trip[5][3:5])
        seconds = int(trip[5][6:8])    
        arrtime = datetime.time(hours, minutes, seconds)

        departed = datetime.datetime.combine(today, deptime)
        arrived = datetime.datetime.combine(today, arrtime)
        traveltime = round((arrived - departed).total_seconds()/60)
        
        hours = int(trip[4][0:2])
        minutes = int(trip[4][3:5])
        seconds = int(trip[4][6:8])
        stopdep = datetime.time(hours, minutes, seconds)

        hours = int(stoparrival[0:2])
        minutes = int(stoparrival[3:5])
        seconds = int(stoparrival[6:8])
        stoparr = datetime.time(hours, minutes, seconds)

        stopoverstation = trip[0]
        stoptime = round((datetime.datetime.combine(today, stopdep) - datetime.datetime.combine(today, stoparr)).total_seconds()/60)
        
        freq = min([depfrequency, trip[3]]) 

        checklist.append([freq, traveltime, stopoverstation, stoptime])

    checklist.sort(key=itemgetter(0))
    modetimes = checklist[-3:]

    modetimes.sort(key=itemgetter(2))
    
    if len(modetimes)> 0:
        modetime = modetimes[0]
        return modetime
    else:
        return modetimes

#establish database connection
to = datetime.datetime.now()
try:
    conn = psycopg2.connect("dbname='fswork' user='fsuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#pull trains, times and stations out of database

loadtimes = """SELECT * FROM traveltimes"""
loadvalidtimes = """SELECT * FROM traveltimes WHERE firsttimeto IS NOT NULL"""
loadstations = """SELECT * FROM stations"""

cur.execute(loadtimes)
traveltimestuple = cur.fetchall()
traveltimes = []
for row in traveltimestuple:
    traveltimes.append(list(row))

cur.execute(loadvalidtimes)
validtraveltuple = cur.fetchall()
validtravel = []
validtraveltimes = {}
for row in validtraveltuple:
    validtravel.append([row[1],row[2]])
    
    combistring = '%s%s' % (row[1], row[2])
    try:
        validtraveltimes[combistring] = max(int(row[3]), int(row[5]), int(row[7]))
    except:
        validtraveltimes[combistring] = int(row[3])

cur.execute(loadstations)
stationstuple = cur.fetchall()
stations = []
for row in stationstuple:
    stations.append(list(row))

stopdict = {}
for station in stations:
    stopdict[station[2]] = [station[-1],station[-2]]

te = datetime.datetime.now()
print("Data loaded")
print("Time spent: %s \n" % (te-to))

#pull final results from database to verify progress
loadalltimes = """SELECT departfrom,arriveat FROM traveltimes2"""

cur.execute(loadalltimes)
alltimes = cur.fetchall()

#find travel times with stopover
for trip in traveltimes[700:750]:
    depstation = trip[1]
    arrstation = trip[2]
    directtravellist = []
    if trip[4] is not None:
        directtravellist.append(trip[3])
    if trip[6] is not None:
        directtravellist.append(trip[5])
    if trip[8] is not None:
        directtravellist.append(trip[7])
    listoftraveltimes = []

    #verify if already in dbase
    if (depstation,arrstation) in alltimes:
        print("already committed %s - %s" % (depstation, arrstation))
    else:
        to = datetime.datetime.now()
        print("verifying %s - %s" % (depstation, arrstation))
        
        #verify what trains depart from depstation
        deptostoplist = []
        for pair in validtravel: # select routes from the depstation
            if depstation == pair[0]:
                combistring = '%s%s' % (pair[0], pair[1])
                combitimes = validtraveltimes[combistring]
                if all(int(i) > combitimes for i in directtravellist): #only if traveltime to 1st stop is less than direct travel 
                    deptostoplist.append(pair[1])
        stoptoarrlist = []
        for pair in validtravel:
            if pair[0] in deptostoplist and pair[1] == arrstation:
                stoptoarrlist.append(pair[0])
         
        #verify what trains depart from depstatoin to selected routes 
        if stoptoarrlist:
            if len(stoptoarrlist)>1:
                firsttriplist = []
                for stop in stoptoarrlist:
                    firsttriplist = trainimporter(depstation, stop)
                    for firsttrip in firsttriplist:
                        secondtriplist = []
                        deptime = firsttrip[4]
                        depfrequency = firsttrip[3]
                        stoparrival = firsttrip[5]
                        stopdummy = stopdict.get(stop)[0]
                        stoptime = stopdict.get(stop)[1]
                    
                        #verify what trains depart 30 minutes from stopoverstation if stopoverstation
                        if stopdummy == '1':
                            secondtriplist = stopimporter(stop, arrstation, stoparrival, stoptime)
                            if modefinder(deptime, stoparrival, depfrequency, secondtriplist):
                                listoftraveltimes.append(modefinder(deptime, stoparrival, depfrequency, secondtriplist))

        
            else:
                firsttriplist = []
                firsttriplist = trainimporter(depstation, stop)
                secondtriplist = []
                stoparrival = firsttrip[5]
                stopdummy = stopdict.get(stop)[0]
                stoptime = stopdict.get(stop)[1]
                    
                #verify what trains depart 30 minutes from stopoverstation if stopoverstation
                if stopdummy == '1':
                    secondtriplist = stopimporter(stop, arrstation, stoparrival, stoptime)
                    if modefinder(deptime, stoparrival, depfrequency, secondtriplist):
                        listoftraveltimes.append(modefinder(deptime, stoparrival, depfrequency, secondtriplist))

            listoftraveltimes.sort(key=itemgetter(0))
            print(listoftraveltimes[-1])

    te = datetime.datetime.now()
    print("Time spent: %s \n" % (te-to))




"""
        combos = []
        deptostop = []
        stoplist = []
        for row in validtravel:
            if row[0] == depstation:
                combos.append(row[1])
        for c in combos:
            stopstation = c
            deptostop = trainimporter(depstation, stopstation)
        for d in deptostop:
            darrival = d[5]
            stopdummy = stopdict.get(stopstation)[0]
            stoptime = stopdict.get(stopstation)[1]
            if stopdummy == '1':
                stoplist = stopimporter(stopstation, arrstation, darrival, stoptime)
                for stop in stoplist:
                    print(d, stop)
        te = datetime.datetime.now()
        print("Time spent: %s \n" % (te-to))


        

        deplist = traindict[depstation] # create list of all trains that depart from depfrom station
        for dep in deplist:
            stopstation = dep[2]
            stoptime = datetime.datetime.combine(today, timerconverter(dep[6]))
            if dep[-1]:
                stopoverlist = traindict[stopstation]
                for stopover in stopoverlist:
                    endstation = stopover[2]
                    starttime = datetime.datetime.combine(today, timerconverter(stopover[5]))
                    if starttime < (stoptime + datetime.timedelta(minutes=30)) and arrstation == endstation:
                        print(dep, stopover)

"""
               