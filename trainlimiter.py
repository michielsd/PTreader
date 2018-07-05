import psycopg2

#establish database connection
try:
    conn = psycopg2.connect("dbname='fswork' user='fsuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#open files
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


rosterlist = []
for line in ft:
    if line.startswith("#"):
        keyname = line[0:6]
    elif line.startswith("0") or line.startswith("1"):
        roster = line[0:364]
        freq = roster.count('1')
        rosterlist.append([keyname, roster, freq])

for train in trains[0:100]:
    print(train)
