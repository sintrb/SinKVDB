SinKVDB
=======

A Python Key-Value Database.
=======

Now, SinKVDB support: None, boolean, integer, float, string, dictionary, list, tuple.

You can use SinKVDB like this follow:

* Create a MySQL connection
> con = MySQLdb.connect(host='127.0.0.1', user='username', passwd='passwd', db='dbname', port=3306)

* Then use this connection to create a KVDB
> kvdb = SinKVDB(dbcon=con, table='trb', tag='test', reset=True)

* After this, you can use kvdb as a simple dictionary
> kvdb['name'] = 'Robin'
> kvdb['age'] = 23
> kvdb['weight'] = 67.5
> kvdb['email'] = ['sintrb@gmail.com', 'trbbadboy@qq.com']
> kvdb['morinfo'] = {'giturl':'https://github.com/sintrb/'}