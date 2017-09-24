#!/usr/bin/python

import sqlite3

conn = sqlite3.connect('TID.db')
print ("Opened database successfully")

cursor = conn.execute("SELECT TID,REVISION,FILE,LINE from Temporal")
for row in cursor:
   print (row[0])
   print (row[1])
   print (row[2])
   print (row[3])
   print ()
   print ()

print ("Operation done successfully")
conn.close()
