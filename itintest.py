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



grori = 'gn'
grdest = ['gn','alm', 'amf', 'asd', 'asdz', 'apg', 'asn', 'nsch', 'bf', 'bdm', 'bl', 'bp', 'dwe', 'dz', 'dzw', 'gvc', 'dron', 'fwd', 'gd', 'gk', 'gerp', 'gnn', 'hrn', 'hgv', 'hgz', 'hdg', 'kpnz', 'kw', 'leer', 'lw', 'lwc', 'ledn', 'lls', 'lp', 'mth', 'mp', 'rd', 'rta', 'rtd', 'spm', 'swd', 'sda', 'shl', 'stm', 'uhz', 'uhm', 'ust', 'ut', 'vdm', 'wfm', 'wr', 'ws', 'wsm', 'zb', 'zh', 'zl']

#METHOD ONE: PULLED INDIVIDUALLY
to = datetime.datetime.now()

travellist = []
for dest in grdest[1:]:
    findpair = """SELECT departfrom, arriveat, time1, freq1, trainsperday FROM traveltimes WHERE departfrom = '%s' AND arriveat = '%s' AND firsttimeto IS NOT NULL""" % (grori, dest)
    cur.execute(findpair)
    firstleg = list(cur.fetchone())
    travellist.append(firstleg)

travellist.sort(key=itemgetter(2))
bestitin = travellist[0]

te = datetime.datetime.now()
print('Individual method complete. Time spent: %s' % (te-to))
print('Result:')
print(bestitin)

#METHOD TWO: PULLED AS A GROUP AND RANKED
to = datetime.datetime.now()

firstpart = """SELECT departfrom, arriveat, time1, trainsperday FROM traveltimes WHERE firsttimeto IS NOT NULL AND departfrom = '%s' AND arriveat IN ("""
secondpart = ((len(grdest)-1) * "'%s', ")[:-2]
thirdpart = """) ORDER BY time1 LIMIT 1"""

sqlrequest = "(" + firstpart + secondpart + thirdpart + ")"

findpair = sqlrequest % (tuple(grdest))
print(findpair)
cur.execute(findpair)
firstleg = cur.fetchone()

te = datetime.datetime.now()
print('Multi pull method complete. Time spent: %s' % (te-to))
print('Result:')
print(firstleg)


# % (grori, dest)