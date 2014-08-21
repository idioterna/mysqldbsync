#!/usr/bin/env python
#
#  Makes it possible to compare and resync a MySQL replication
#  cluster with missing rows (but not changed rows!)
#
#  Be careful.
#

import MySQLdb, sys, operator, getpass

args = sys.argv[1:]
if len(args) < 3:
	print '''Usage: %s <host1> <host2> <database> [table1] [table2] ... [--dry-run]

	--dry-run prevents changes from being made to data''' % sys.argv[0]
	sys.exit(1)

password = getpass.getpass('Password: ')

host1 = args[0]
host2 = args[1]
dbname = args[2]

db1 = MySQLdb.connect(host1, 'root', password, dbname)
db2 = MySQLdb.connect(host2, 'root', password, dbname)

c1 = db1.cursor()
c2 = db2.cursor()
c1r = db1.cursor()
c1r.execute("set sql_log_bin=0") # don't write these to binlog
c2r = db2.cursor()
c2r.execute("set sql_log_bin=0") # don't write these to binlog

dry_run = False
while '--dry-run' in args:
	args.remove('--dry-run')
	dry_run = True

# select table names and figure out their primary keys
table_names = args[3:]
if table_names:
	c1.execute("select table_name, column_name, ordinal_position from information_schema.key_column_usage where constraint_name = 'PRIMARY' and table_schema = %s and table_name in (%s)", [dbname, ','.join(["%s" % x for x in table_names])])
else:
	c1.execute("select table_name, column_name, ordinal_position from information_schema.key_column_usage where constraint_name = 'PRIMARY' and table_schema = %s", [dbname])

primarykeys = {}
rowcount = {}

for row in c1.fetchall():
	if row[2] > 1:
		primarykeys[row[0]] += [row[1]]
	else:
		primarykeys[row[0]] = [row[1]]
	# also get estimates on table sizes
	try:
		c1r.execute("select table_rows from information_schema.tables where table_name = %s and table_schema = %s", [row[0], dbname])
		rowcount[row[0]] = c1r.fetchall()[0][0]
	except:
		pass

# by default, sort tables so that smaller tables are synced first
sorted_sizes = sorted(rowcount.iteritems(), key=operator.itemgetter(1))
tablelist = [x[0] for x in sorted_sizes]
for table in primarykeys:
	if table not in rowcount:
		tablelist.append(table)

try:
	for table in tablelist:
		# select all primary key columns from table in db1
		print 'doing', table
		tablekeys = {}
		cnt = 0
		keys = primarykeys[table]
		c1.execute("select %s from %s" % (', '.join(keys), table))
		for row in c1.fetchall():
			tablekeys[row] = 1
			cnt += 1
		print cnt, 'done on', host1, table
		cnt = 0
		icnt = 0

		# select all primary key columns from table in db2
		c2.execute("select %s from %s" % (', '.join(keys), table))
		for row in c2.fetchall():
			# check if a matching row already exists in db1 and skip it
			if row in tablekeys:
				del tablekeys[row]
			# otherwise select the entire row from db2 and insert it into db1
			else:
				where = ', '.join(['%s = %%s' % (x,) for x in keys])
				if not dry_run:
					c2r.execute("select * from %s where %s" % (table, where), row)
					fullrow = c2r.fetchone()
					c1r.execute("insert into %s values (%s)" % (table, ', '.join(['%s' for x in range(len(fullrow))])), fullrow)
				icnt+=1
				if icnt % 1000 == 0:
					db1.commit()
			cnt+=1

		# if we inserted any rows, do a commit here
		if icnt:
			if not dry_run:
				db1.commit()
			print icnt, 'inserted into', host1
		print cnt, 'done on', host2, table
		cnt = 0

		# insert entire rows from db1 that weren't matched when reading db2 into db2
		for keyleft in tablekeys:
			where = ', '.join(['%s = %%s' % x for x in keys])
			if not dry_run:
				c1r.execute("select * from %s where %s" % (table, where), keyleft)
				fullrow = c1r.fetchone()
				c2r.execute("insert into %s values (%s)" % (table, ', '.join(['%s' for x in range(len(fullrow))])), fullrow)
			cnt+=1
			if cnt % 1000 == 0:
				db2.commit()

		# commit if we inserted anything
		if cnt:
			if not dry_run:
				db2.commit()
			print cnt, 'inserted into', host2

except:
	# print last executed queries to help with debugging
	print c1._last_executed
	print c2._last_executed
	print c1r._last_executed
	print c2r._last_executed
	# and roll back what you can
	db1.rollback()
	db2.rollback()
	raise

