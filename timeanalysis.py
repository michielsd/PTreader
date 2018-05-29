
import psycopg2
import datetime
import itertools

#function to convert postgres time to datetime time
def trainimporter(depstation, arrstation):
    loadtrains = """SELECT depstation, arrstation, train, frequency, deptime, arrtime, difference FROM trains WHERE depstation='%s' AND arrstation='%s'""" % (depstation, arrstation)
    cur.execute(loadtrains)
    trainstuple = cur.fetchall()
    trains = []
    for row in trainstuple:
        trains.append(list(row))

    return trains

def stopimporter(depstation, arrstation, arrtime,stoptime):
    today = datetime.date.today()
    hours = int(arrtime[0:2])
    minutes = int(arrtime[3:5])
    seconds = int(arrtime[6:8])  

    firstdeptime = datetime.time(hours, minutes, seconds)
    workingtime = datetime.datetime.combine(today, firstdeptime)
    lastdeptime = (workingtime + datetime.timedelta(minutes=int(stoptime)) + datetime.timedelta(minutes=30)).time()
    
    loadtrains = """SELECT depstation, arrstation, train, frequency, deptime, arrtime, difference FROM trains WHERE depstation='%s' AND arrstation='%s' AND deptime BETWEEN '%s' AND '%s'""" % (depstation, arrstation, firstdeptime, lastdeptime)
    cur.execute(loadtrains)
    trainstuple = cur.fetchall()
    trains = []
    for row in trainstuple:
        trains.append(list(row))

    return trains    

def timerconverter(posttime):
    hours = int(posttime[0:2])
    minutes = int(posttime[3:5])
    seconds = int(posttime[6:8])
    return(datetime.time(hours, minutes, seconds))

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
for row in validtraveltuple:
    validtravel.append([row[1],row[2]])

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
for trip in traveltimes[700:705]:
    depstation = trip[1]
    arrstation = trip[2]
    directtravellist = []
    if trip[4] is not None:
        directtravellist.append(trip[4])
    if trip[6] is not None:
        directtravellist.append(trip[6])
    if trip[8] is not None:
        directtravellist.append(trip[8])

    #verify if already in dbase
    if (depstation,arrstation) in alltimes:
        print("already committed %s - %s" % (depstation, arrstation))
    else:
        to = datetime.datetime.now()
        print("verifying %s - %s" % (depstation, arrstation))
        
        #verify what trains depart from depstation
        departurelist = []
        for pair in validtravel: # select routes from the depstation
            if depstation == pair[0]:
                departurelist.append(pair[1])
        depintlist = []
        
        #verify what trains depart from depstatoin to selected routes 
        for stop in departurelist:
            depintlist = trainimporter(depstation, stop)
            for end in depintlist:
                if all(i > end[-1] for i in directtravellist): #only if traveltime to 1st stop is less than direct travel
                    destlist = []
                    stoparrival = end[5]
                    stopdummy = stopdict.get(stop)[0]
                    stoptime = stopdict.get(stop)[1]
                    
                    #verify what trains depart 30 minutes from stopoverstation if stopoverstation
                    if stopdummy == '1':
                        destlist = stopimporter(stop, arrstation, stoparrival, stoptime)
                        print(end,destlist)
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
               