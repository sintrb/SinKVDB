SinKVDB
=======

A Python Key-Value Database.
=======

Now, SinKVDB support: None, boolean, integer, float, string, dictionary, list, tuple.

You can use SinKVDB like this follow:

* Create a MySQL connection
<pre><code> con = MySQLdb.connect(host='127.0.0.1', user='username', passwd='passwd', db='dbname', port=3306)</code></pre>

* Then use this connection to create a KVDB
<pre><code>kvdb = SinKVDB(dbcon=con, table='trb', tag='test', reset=True)</code></pre>

* After this, you can use kvdb as a simple dictionary
<pre><code>kvdb['name'] = 'Robin'
kvdb['age'] = 23
kvdb['weight'] = 67.5
kvdb['email'] = ['sintrb@gmail.com', 'trbbadboy@qq.com']
kvdb['morinfo'] = {'giturl':'https://github.com/sintrb/'}</code></pre>

* Maybe commit is necessary
<pre><code>kvdb.dbcon.commit()</code></pre>