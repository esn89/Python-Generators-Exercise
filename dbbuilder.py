#!/usr/bin/env python
import MySQLdb

# INFORMATION TO YOUR DATABASE MUST BE ENTERED HERE
mydb = MySQLdb.connect(host='localhost',
                       user='root',
                       passwd='lobster',
                       db='mydb')
cursor = mydb.cursor()

try:
    cursor.execute("DROP TABLE IF EXISTS m;")
except MySQLdb.OperationalError, MySQLdb.Warning:
    print "Table never existed in the first place"

try:
    # Don't need PK cuz we need to count our dupes here
    cursor.execute("CREATE TABLE m (addr VARCHAR(255) NOT NULL);")
    print "Success"
except MySQLdb.OperationalError:
    print "Error creating 'm' table"

# now we create the m table so I can use it


def getrows(filename):
    with open(filename) as f:
        for line in f:
            yield line

for item in getrows('dummy.txt'):
    item = item.rstrip()
    query = 'INSERT INTO m VALUES ("%s");' % item
    cursor.execute(query)
mydb.commit()
cursor.close()
print "Done, successfully created 'm' table"
