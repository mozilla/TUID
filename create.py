#!/usr/bin/python

import sqlite3

conn = sqlite3.connect('TID.db')

print("Opened database successfully")

for i in range(0,10000):
	conn.execute("INSERT INTO Temporal (REVISION,FILE,LINE) \
		VALUES ('XXXXXXXXXXXX','browser/components/places/content/treeHelpers.js',"+ str(i) +")");

conn.commit()
print ("Records created successfully");
conn.close()
