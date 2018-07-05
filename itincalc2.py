import psycopg2
from operator import itemgetter
import datetime
import itertools

#establishing database connection and readying data
to = datetime.datetime.now()
try:
    conn = psycopg2.connect("dbname='fswork' user='fsuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

loadstopovertimes = """SELECT stopstation, depstation, arrstation, stoptime FROM stopovertimes"""

cur.execute(loadstopovertimes)
stopovertimestuple = cur.fetchall()
stopovertimes = []
for row in stopovertimestuple:
    stopovertimes.append(list(row))

loadtraveltimes = """SELECT departfrom, arriveat, time1 FROM traveltimes WHERE firsttimeto IS NOT NULL"""

cur.execute(loadtraveltimes)
traveltimestuple = cur.fetchall()
traveltimes = []
for row in traveltimestuple:
    traveltimes.append(list(row))

loadstations = """SELECT code, name, lat, lon, country, stoptime, stopstat FROM stations"""

cur.execute(loadstations)
stationstuple = cur.fetchall()
stations = []
stationcodes = []
for row in stationstuple:
    stations.append(list(row))
    stationcodes.append(row[0])

loaditineraries = """SELECT depname, arrname FROM timeperroute"""
insertinerary = """ INSERT INTO timeperroute(depname, deplat, deplon, arrname, arrlat, arrlon, traveltime, trainsperday, stop1name, stop1lat, stop1lon, stop2name, stop2lat, stop2lon)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

cur.execute(loaditineraries)
itinerariestuple = cur.fetchall()
itineraries = []
for row in itinerariestuple:
    itineraries.append(list(row))

te = datetime.datetime.now()
print("Data loaded")
print("Time spent: %s \n" % (te-to))

#Processing data
#creating pairs
stationpairs = [] 
for subset in itertools.combinations(stationcodes, 2):
    stationpairs.append(subset) 

#creating list per station of where you can travel to from single station
destinationdict = {}
for station in stationcodes:
    destinationlist = []
    for trip in traveltimes:
        if station == trip[0]:
            destinationlist.append(trip[1])
    destinationdict[station] = destinationlist

#create dict of stations
stationdict = {}
reversestationdict = {}
for station in stations:
    stationdict[station[0]] = station[1:]
    reversestationdict[station[1]] = station[0]

traveldict = {}
for trip in traveltimes:
    tripname = '%s-%s' % (trip[0],trip[1])
    traveldict[tripname] = int(trip[2])

# analyse which are already committed
for row in itineraries:
    row[0] = reversestationdict[row[0]]
    row[1] = reversestationdict[row[1]]

to = datetime.datetime.now()
print("Data processed")
print("Time spent: %s \n" % (to-te))

#performing analysis
for pair in stationpairs:
    depstation = pair[0]
    arrstation = pair[1]
    print("verifying %s - %s" % (depstation, arrstation))
    to = datetime.datetime.now()
    
    if list(pair) in itineraries:

        itinerarylist = []

        if depstation == arrstation:
            break

        onetrain = []
        twotrains = []
        directtrip = []

        #track for immediate connection
        if arrstation in destinationdict[depstation]:

            findpair = """SELECT departfrom, arriveat, time1, freq1, trainsperday FROM traveltimes WHERE departfrom = '%s' AND arriveat = '%s' AND firsttimeto IS NOT NULL""" % (depstation, arrstation)
            cur.execute(findpair)
            directtrip = list(cur.fetchone())

            directtrip[2] = int(directtrip[2])

            onetrain = [depstation, arrstation, directtrip[2], directtrip[4],"",""]
            itinerarylist.append(onetrain)

        #track for two leg connection
        stoplist = []
        for stop1 in destinationdict[depstation]:
            if arrstation in destinationdict[stop1] and stop1 != arrstation:
                tripname = '%s-%s' % (depstation, stop1)
                if onetrain == [] or traveldict[tripname] < int(onetrain[2]):
                    stoplist.append(stop1)

        #obtain traveltimes firstleg
        if len(stoplist) > 0:
            firstlegstoplist = stoplist
            firstlegstoplist.insert(0, depstation)
            
            firstpart = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE firsttimeto IS NOT NULL AND departfrom = '%s' AND arriveat IN ("""
            secondpart = ((len(stoplist)-1) * "'%s', ")[:-2]

            sqlrequest = "(" + firstpart + secondpart + "))"
            findpairs = sqlrequest % (tuple(firstlegstoplist))
            cur.execute(findpairs)

            firstltup = cur.fetchall()
            firstleg = []
            for trip in firstltup:
                firstleg.append(list(trip))

        #obtain stoptimes

            stopoverlist = stoplist
            stopoverlist.insert(0, arrstation)
            stopoverlist.insert(0, depstation)

            firstpart = """SELECT stoptime FROM stopovertimes WHERE depstation = '%s' AND arrstation = '%s' AND stopstation IN ("""
            secondpart = ((len(stoplist)-2) * "'%s', " )[:-2]

            sqlrequest = "(" + firstpart + secondpart + "))"
            findpairs = sqlrequest % (tuple(stopoverlist))
            cur.execute(findpairs)

            stoptup = cur.fetchall()
            stopovers = []
            for trip in stoptup:
                stopovers.append(list(trip))

        #obtain traveltimes secondleg

            secondlegstoplist = stoplist
            secondlegstoplist.insert(0, arrstation)

            firstpart = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE firsttimeto IS NOT NULL AND arriveat = '%s' AND departfrom IN ("""
            secondpart = ((len(stoplist)-1) * "'%s', ")[:-2]

            sqlrequest = "(" + firstpart + secondpart + "))"
            findpairs = sqlrequest % (tuple(secondlegstoplist))
            cur.execute(findpairs)

            secondltup = cur.fetchall()
            secondleg = []
            for trip in secondltup:
                secondleg.append(list(trip))

        #obtain best secondleg result
            for i,fl in enumerate(firstleg):
                fl.extend(stopovers[i])
                fl.extend(secondleg[i])

            for row in firstleg:
                row.append((int(row[2]) + int(row[4]) + int(row[7])))

            firstleg.sort(key=itemgetter(-1))
            selected = firstleg[0]
            trainsperday = min(int(selected[3]), int(selected[8]))

            twotrains = [depstation, arrstation, selected[-1], trainsperday, selected[1], ""]
            itinerarylist.append(twotrains)

        #three leg trip:
        if directtrip:
            pass
        else:
            stop1list = []
            stop2list = []
            for stop1 in destinationdict[depstation]:
                for stop2 in destinationdict[stop1]:
                            if arrstation in destinationdict[stop2] and stop1 != arrstation and stop2 != arrstation:
                                trip1 = '%s-%s' % (depstation, stop1)
                                trip2 = '%s-%s' % (stop1, stop2)
                                if twotrains == [] or traveldict[trip1] < int(twotrains[2]) and traveldict[trip2] < int(twotrains[2]):
                                    stop1list.append(stop1)
                                    stop2list.append(stop2)

            if len(stop1list) > 0 and len(stop2list) > 0:
                
                #fold stoplists
                combostop = []
                for i, stop in enumerate(stop1list):
                    combostop.append([stop, stop2list[i]])

                #firststop
                firstlegstoplist = stop1list
                firstlegstoplist.insert(0, depstation)
                
                firstpart = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE firsttimeto IS NOT NULL AND departfrom = '%s' AND arriveat IN ("""
                secondpart = ((len(stop1list)-1) * "'%s', ")[:-2]

                sqlrequest = "(" + firstpart + secondpart + "))"
                findpairs = sqlrequest % (tuple(firstlegstoplist))
                cur.execute(findpairs)

                firstltup = cur.fetchall()
                firstleg = []
                for trip in firstltup:
                    firstleg.append(list(trip))

                #find the stops
                times1list = []
                times2list = []
                for stop in combostop:

                    findstop1time = """SELECT stoptime FROM stopovertimes WHERE depstation = '%s' AND stopstation = '%s' AND arrstation = '%s'""" % (depstation, stop[0], stop[1])
                    cur.execute(findstop1time)
                    firststoptime = cur.fetchone()
                    if firststoptime:
                        times1list.append(firststoptime)

                    findstop2time = """SELECT stoptime FROM stopovertimes WHERE depstation = '%s' AND stopstation = '%s' AND arrstation = '%s'""" % (stop[0], stop[1], arrstation)
                    cur.execute(findstop2time)
                    secondstoptime = cur.fetchone()
                    if secondstoptime:
                        times2list.append(secondstoptime)

                #find second leg
                secondleg = []
                for stop in combostop:
                    
                    findpair = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE departfrom = '%s' AND arriveat = '%s' AND firsttimeto IS NOT NULL""" % (stop[0], stop[1])
                    cur.execute(findpair)
                    secondltup = cur.fetchone()
                    if secondltup:
                        secondleg.append(list(secondltup))

                #third stop
                thirdlegstoplist = stop2list
                thirdlegstoplist.insert(0, arrstation)

                firstpart = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE firsttimeto IS NOT NULL AND arriveat = '%s' AND departfrom IN ("""
                secondpart = ((len(stop2list)-1) * "'%s', ")[:-2]

                sqlrequest = "(" + firstpart + secondpart + "))"
                findpairs = sqlrequest % (tuple(thirdlegstoplist))
                cur.execute(findpairs)

                thirdltup = cur.fetchall()
                thirdleg = []
                for trip in thirdltup:
                    thirdleg.append(list(trip))

                print(firstleg)
                print(secondleg)
                print(thirdleg)
                print(times1list)
                print(times2list)



                                    

                #obtain traveltimes secondleg




"""

                            if firststoptime and secondstoptime:
                                firststoptime = list(firststoptime)
                                secondstoptime = list(secondstoptime)

                                stopstation1 = firstleg[1]
                                stopstation2 = secondleg[1]
                                totaltime = int(firstleg[2]) + int(secondleg[2]) + int(thirdleg[2]) + int(firststoptime[0]) + int(secondstoptime[0])
                                trainsperday = min(int(firstleg[4]), int(secondleg[4]), int(thirdleg[4]))

                                threetrains = [depstation, arrstation, totaltime, trainsperday, stopstation1, stopstation2]
                                itinerarylist.append(threetrains)

        if itinerarylist:
            itinerarylist.sort(key=itemgetter(2))
            bestitin = itinerarylist[0]
            inputlist = [
                stationdict[bestitin[0]][0],
                stationdict[bestitin[0]][1],
                stationdict[bestitin[0]][2],
                stationdict[bestitin[1]][0],
                stationdict[bestitin[1]][1],
                stationdict[bestitin[1]][2],
                bestitin[2],
                bestitin[3],
                "",
                "",
                "",
                "",
                "",
                ""
            ]

            if bestitin[4]:
                inputlist[8] = stationdict[bestitin[4]][0]
                inputlist[9] = stationdict[bestitin[4]][1]
                inputlist[10] = stationdict[bestitin[4]][2]

            if bestitin[5]:
                inputlist[11] = stationdict[bestitin[5]][0]
                inputlist[12] = stationdict[bestitin[5]][1]
                inputlist[13] = stationdict[bestitin[5]][2]

            cur.execute(insertinerary, inputlist)
            conn.commit()
            te = datetime.datetime.now()
            print("committed %s - %s in %s" % (inputlist[0], inputlist[3],(te-to)))


con.close()

"""