#!/usr/bin/python

# this script skips "duplicate key" rows to bring replication back in sync
# after manually inserting missing data

import sys, MySQLdb, time, getpass

password = getpass.getpass('Password: ')

db1 = MySQLdb.connect(sys.argv[1], 'root', password)
db2 = MySQLdb.connect(sys.argv[2], 'root', password)

try:
	master_conn_1 = sys.argv[3]
	master_conn_2 = sys.argv[4]
except KeyError:
	master_conn_1 = master_conn_2 = None

while True:

	c1 = db1.cursor()
	if master_conn_1: c1.execute("set default_master_connection = '%s'" % master_conn_1)
	c1.execute("show slave status")
	row1 = c1.fetchall()
	if row1[0][18] == 1062L:
		c1.execute("SET GLOBAL sql_slave_skip_counter = 1")
		c1.execute("start slave")
		print 'skipped 1 in 1'

	c2 = db2.cursor()
	if master_conn_2: c2.execute("set default_master_connection = '%s'" % master_conn_2)
	c2.execute("show slave status")
	row2 = c2.fetchall()
	if row2[0][18] == 1062L:
		c2.execute("SET GLOBAL sql_slave_skip_counter = 1")
		c2.execute("start slave")
		print 'skipped 1 in 2'

	time.sleep(0.2)
	print

