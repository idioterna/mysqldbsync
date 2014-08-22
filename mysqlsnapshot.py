#!/usr/bin/python
#
#  python mysqlsnapshot.py localhost /dev/VG/LV [--slave]
#
#  --slave (or mariadb's slave connection name) also prints
#  slave's metadata you may want to inspect when cloning slaves
#

import MySQLdb, sys, os, time, datetime, getpass
import MySQLdb.cursors

try:
	host = sys.argv[1]
	volume = sys.argv[2]
except IndexError:
	print '''python %s <host> <LVM volume> [--slave]

--slave (or mariadb's slave connection name) also prints
slave's metadata you may want to inspect when cloning slaves''' % sys.argv[0]
	sys.exit(1)

try:
	slave = sys.argv[3]
except IndexError:
	slave = None

if os.path.ismount(volume):
	btrfs = True
else:
	btrfs = False

password=getpass.getpass()

t = time.time()
def log(m):
        print '%.2f: %s' % (time.time()-t, str(m))

today = time.strftime('%Y-%m-%d')

log('starting...')
db = MySQLdb.connect(host=host, user='root', passwd=password, cursorclass=MySQLdb.cursors.DictCursor)
c = db.cursor()
c.execute("flush tables with read lock")
c.execute("show master status")
print c.fetchall()
if slave is not None:
	if slave and slave != '--slave':
		c.execute("set default_master_connection = %s", slave)
	c.execute("show slave status")
print c.fetchall()
log('flushed and locked...')
os.system('sync')
log('synced...')
if btrfs:
	os.system("mkdir -p %s/.snapshots/" % volume)
	os.system("btrfs sub snap %s %s/.snapshots/mysql-snapshot-%s" % (volume, volume, today))
else:
	os.system("lvcreate -s -L 10G -n mysql-snapshot-" + today + " " + volume)
log('snapshot created...')
c.close()
db.close()
log('released locks...')
