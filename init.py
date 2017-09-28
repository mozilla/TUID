#!/usr/bin/python

import sqlite3

conn = sqlite3.connect('TID.db')
print ("Opened database successfully");

conn.execute('''CREATE TABLE Temporal
         (TID INTEGER PRIMARY KEY     AUTOINCREMENT,
         REVISION CHAR(40)		  NOT NULL,
         FILE TEXT,
		 LINE INT,
		 UNIQUE(REVISION,FILE,LINE));''')
print ("Table created successfully");

conn.close()

