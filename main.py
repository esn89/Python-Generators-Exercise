#!/usr/bin/env python
import MySQLdb
from collections import defaultdict
import datetime

# INFORMATION TO YOUR DATABASE MUST BE ENTERED HERE
mydb = MySQLdb.connect(host='localhost',
                       user='root',
                       passwd='lobster',
                       db='mydb')
cursor = mydb.cursor()

# Generator function to be used later


def iter_row(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row


# I understand that the m table is updated daily, and I can just use
# an SQL statement to query those that correspond to today's date.
# However, I do not have a perisistent database which has been running for 30
# days or more, so for this purpose, I will query the entire m table so
# that I will have emails with dates from 30 days ago, to work with.

# Under normal circumstances, I can just make a query that grabs entries which
# match today's date and insert it into the dcount table
# without the need to create a new dcount table everytime.

try:
    cursor.execute("DROP TABLE IF EXISTS dcount;")
except MySQLdb.OperationalError, MySQLdb.Warning:
    print "Table never existed in the first place"

try:
    query = "CREATE TABLE dcount( \
                    date    DATE, \
                    domain VARCHAR(255) NOT NULL, \
                    total INT NOT NULL, \
                    PRIMARY KEY(date, domain));"
    cursor.execute(query)
    mydb.commit()
    print "Done, created dcount table"
except MySQLdb.OperationalError:
    print "Failed to create DB"

# read in from the m one, and into the daily one it goes.
# if we find a duplicate, just accumulate
with mydb:
    cursor.execute("SELECT * FROM m;")
    rows = cursor.fetchall()
    for row in rows:
        formatted = row[0].split(',')
        date = formatted[0]
        domain = formatted[1].split('@')[1]
        query = 'INSERT INTO dcount (date, domain, total) \
                        VALUES ("%s", "%s", %s) ON  \
                        DUPLICATE KEY UPDATE total=total+1;' % (date, domain, "1")
        cursor.execute(query)

# Now is today's date and monthago is 30 days earlier
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d")
diff = datetime.timedelta(30)
monthago = datetime.date.today() - datetime.timedelta(days=30)

# Raw list contains all the queries in tuple form,
# between today and 30 days ago
rawlist = []

# Before the date is the statistics from the beginning of our very first
# database entry, up until 30 days ago
beforethedate = []


def getlastmonth():
    with mydb:
        query = "SELECT date, domain, total \
            FROM dcount WHERE date BETWEEN '%s' and '%s';" % (monthago, now)
        cursor.execute(query)
        for row in iter_row(cursor, 10):
            date = str(row[0])
            dom = str(row[1])
            tot = int(row[2])
            final = (date, dom, tot) rawlist.append(final)
# Call this function I just made:
getlastmonth()


def getbeforelastmonth():
    with mydb:
        query = "SELECT domain, total \
            FROM dcount WHERE date < " + "'" + str(monthago) + "'" + ";"
        cursor.execute(query)
        for row in iter_row(cursor, 10):
            dom = str(row[0])
            tot = int(row[1])
            final = (dom, tot)
            beforethedate.append(final)

getbeforelastmonth()

# 'sumofbefore' contains ALL the tallied up stats per site from the beginning
# of time up until 30 days ago.
sumofbefore = defaultdict(list)
for domain, total in beforethedate:
    sumofbefore[domain].append(total)
talliedbefore = ({k: sum(v) for k, v in sumofbefore.iteritems()})

# read in all the tuples:
templist = []
for l in rawlist:
    final = (l[1], l[2])
    templist.append(final)

d = defaultdict(list)
for domain, total in templist:
    d[domain].append(total)

sumformonth = ({k: sum(v) for k, v in d.iteritems()})

# We have the total stats from beginning of time and the ones accumulated from
# the past thirty days:
# Let's generate this report

# I define growth as the total of users that went to the site this month
# compared to the total before the month
domainandgrowth = []
for item in sumformonth:
    if item in talliedbefore:
        growth = (float(sumformonth[item]) / float(talliedbefore[item])) * 100
        growth = round(growth, 2)
        pair = (item, growth)
        domainandgrowth.append(pair)
    if item not in talliedbefore:
        # If it was not found before, that means that it
        # is a new site we suddently gained popularity:
        # print "not in talliedbefore"
        growth = round(sumformonth[item] / 1 * 100, 2)
        pair = (item, growth)
        domainandgrowth.append(pair)

# This sorted 'domaingrowth' contains the domains in sorted growth values with
# the highest growth at the head of the list
finalist = sorted(domainandgrowth, key=lambda x: x[1], reverse=True)

# This writes my result to a file named 'top50.txt'
f = open('top50.txt', 'w')
f.write("The Top 50 domains this past 30 days by growth percentage:\n\n")
for ele in finalist[:50]:
    output = "%s %4.1f" % (ele[0].ljust(30, '.'), ele[1])
    f.write(output + '\n')
f.close()
