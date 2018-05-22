
import psycopg2

#establish database connection
try:
    conn = psycopg2.connect("dbname='fswork' user='fsuser' host='localhost' password='123456'")
except:
    print("Database connection failed")

cur = conn.cursor()

#pull trains and times out of database
loadtrains = """SELECT * FROM trains"""
loadtimes = """SELECT * FROM traveltimes"""

cur.execute(loadtrains)
trains = cur.fetchall()

cur.execute(loadtimes)
traveltimes = cur.fetchall()

#pull final results from database to verify progress
loadalltimes = """SELECT departfrom,arriveat FROM traveltimes2"""

cur.execute(loadalltimes)
alltimes = cur.fetchall()

#find travel times with stopover

for trip in traveltimes:
    if (trip[0],trip[1]) in loadalltimes:
        print("already committed %s - %s" % trip[0], trip[1])
    else:
        