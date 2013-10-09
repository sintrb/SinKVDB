#SinKVDB


##About
A Python Key-Value Database.

Now, SinKVDB support: *None*, *boolean*, *integer*, *float*, *string*, *dictionary*, *list*, *tuple*.

##How to use
You can use SinKVDB like this follow:

* Create a MySQL connection.
<pre><code> con = MySQLdb.connect(host='127.0.0.1', user='username', passwd='passwd', db='dbname', port=3306)</code></pre>

* Then use this connection to create a kvdb.
<pre><code>kvdb = SinKVDB(dbcon=con, table='trb', tag='test', reset=True)</code></pre>

* After this, you can use kvdb as a simple dictionary.
<pre><code>kvdb['name'] = 'Robin'
kvdb['age'] = 23
kvdb['weight'] = 67.5
kvdb['email'] = ['sintrb@gmail.com', 'trbbadboy@qq.com']
kvdb['morinfo'] = {'giturl':'https://github.com/sintrb/'}</code></pre>

* At last, maybe commit is necessary. If you set autocommit=True when you creaye KVDB, the follow is unnecessary.
<pre><code>kvdb.commit()</code></pre>

##Advanced Dictionary Functions
There are some advanced things of SinKVDB:
* Like Dictionary's items() function: get all key-value pairs and print it
<pre><code>for (k,v) in kvdb.items():
----print 'key:%s\tval:%s'%(k,v)
</code></pre>

* Cooler items(): get all key-value pairs witch key contain 'i' and print it
<pre><code>for (k,v) in kvdb.items('%i%'):
----print 'key:%s\tval:%s'%(k,v)
</code></pre>

* Of course, you only get keys or values like this.
<pre><code>for k in kvdb.keys():
----print k
# or
for v in kvdb.values():
----print v
</code></pre>

* Delete and Contains is also supported.
<pre><code>del kvdb['name']	#delete key-value pair witch key is 'name'
print 'name' in kvdb # judge key 'name' whether in this KVDB
</code></pre>




