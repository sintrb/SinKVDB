# -*- coding: UTF-8 -*
'''
Created on 2013-9-23

@author: RobinTang
'''

import MySQLdb
import time
import json

__version__ = '1.0'

__TPL_DELETETABLE__ = '''
DROP TABLE IF EXISTS `%s`;
'''

__TPL_CREATETABLE__ = '''
CREATE TABLE IF NOT EXISTS `%s` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `key` char(255) NOT NULL,
  `value` longblob,
  `tag` char(255) DEFAULT NULL,
  `type` char(255) DEFAULT NULL,
  `modifytime` int(11) DEFAULT 0,
  `createtime` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `key_index` (`key`) USING BTREE,
  KEY `tag_index` (`tag`),
  KEY `type_index` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
''' 

class SinKVDB(object):
    '''
    A Python Key-Value Database.
    '''
    maparr = [
            (type(None), 'none', str, eval),
            (type(1),'int', str, int),
            (type(True),'bool', str, eval),
            (type(1.0),'float', str, float),
            (type('s'),'string', str, str),
            (type({}),'dict', json.dumps, json.loads),
            (type([]),'list', json.dumps, json.loads),
            (type(()),'tuple', json.dumps, json.loads),
        ]
    
    
    typemap = dict(((a[0], a[1]) for a in maparr))
    convmap = dict(((a[0], a[2]) for a in maparr))
    valumap = dict(((a[1], a[3]) for a in maparr))
    
    def __init__(self, dbcon=None, table=None, tag=None, cache=True, reset=False, debug = False):
        '''
        A Python Key-Value Database.
        @dbcon Database connection
        @table Data table name
        @tag A tag, in most time, you use this tag to distinguish different KVDB in same table
        @reset Reset the table or not
        @debug In debug mode or not
        '''
        self.dbcon = dbcon
        self.table = table
        self.tag = tag
        self.debug = debug
        self.cache = cache
        self.__pingdb__()
        if reset:
            self.reset_table()
        else:
            self.__create_table__()
        
        # internal cache
        if self.cache:
            self.__cache__ = {}
    
    def __delete_table__(self):
        '''
        Delete the table
        '''
        if self.cache:
            self.__cache__.clear()
        return self.__execsql__(__TPL_DELETETABLE__%self.table)
    
    def __create_table__(self):
        '''
        Create table to store key-value data
        '''
        return self.__execsql__(__TPL_CREATETABLE__%self.table)
        
    def __pingdb__(self):
        '''
        Ping the database connection when the connection was die
        '''
        self.dbcon.ping(True)
        self.dbcur = self.dbcon.cursor()
        
    def __execsql__(self, query, args=None):
        '''
        Execute a SQL query
        '''
        if self.debug:
            if args is not None:
                sql = query % self.dbcon.literal(args)
                print 'query: %s'%sql
            else:
                print 'query: %s'%query
        try:
            return self.dbcur.execute(query, args)
        except:
            self.__pingdb__()
            return self.dbcur.execute(query, args)
        return None
    
    def __sql2array__(self, query, args=None):
        '''
        Execute a SQL query and switch ResultSet to Object array
        '''
        count = self.__execsql__(query, args)
        if count:
            names = [x[0] for x in self.dbcur.description]
            res = []
            allrow = self.dbcur.fetchall()
            for row in allrow:
                obj = dict(zip(names, row))
                res.append(obj)
        else:
            res = []
        return res
    
    def add_one(self, key, value, ctype):
        '''
        Add record by key-value and type
        '''
        return self.__execsql__('INSERT INTO `'+self.table+'`(`key`, `value`, `tag`, `type`, `createtime`, `modifytime`) VALUES(%s, %s, %s, %s, %s, %s)', [key, value, self.tag, ctype, time.time(), time.time()])
    
    def set_one(self, key, value, ctype):
        '''
        Set record by key-value and type
        '''
        return self.__execsql__('UPDATE `'+self.table+'` SET `value`=%s, `tag`=%s, `type`=%s, `modifytime`=%s WHERE `key`=%s', [value, self.tag, ctype, time.time(), key])
    
    def get_one(self, key):
        '''
        Get one record by key
        '''
        objs = self.__sql2array__('SELECT * FROM `'+self.table+'` WHERE `key`=%s and `tag`=%s LIMIT 1', [key, self.tag])
        if objs and len(objs):
            return objs[0]
        else:
            return None
    
    def reset_table(self):
        '''
        Rest the KVDB, it will clear all data store on this table
        '''
        try:
            self.__delete_table__()
            self.__create_table__()
            return True
        except:
            return False
    
    def commit(self):
        '''
        Commit change to Database
        '''
        return self.dbcon.commit()
    
    def __getval__(self, obj):
        '''
        Get the record's value.
        It will switch to list, tuple or a dictionary
        if this record's type is json.
        '''
        if not obj['type'] in SinKVDB.valumap:
            raise Exception('Not support type of: %s'%obj['type'])
        return SinKVDB.valumap[obj['type']](obj['value'])
    
    def __getitem__(self, key):
        if self.cache and key in self.__cache__:
            # return from cache
            return self.__cache__[key]
        
        obj = self.get_one(key)
        if obj:
            return self.__getval__(obj)
        else:
            return None
    
    def __setitem__(self, key, value):
        if not type(value) in SinKVDB.typemap:
            raise Exception('Not support type of: %s'%type(value))
        ctype = SinKVDB.typemap[type(value)]
        cvalue = SinKVDB.convmap[type(value)](value)
        if self.cache:
            self.__cache__[key] = value
        if not self.set_one(key, cvalue, ctype):
            return self.add_one(key, cvalue, ctype)
        else:
            return True
        
    def __delitem__(self, key):
        if self.cache and key in self.__cache__:
            # delete from cache
            del self.__cache__[key]
            
        return self.__execsql__('DELETE FROM `'+self.table+'` WHERE `key`=%s and `tag`=%s LIMIT 1', [key, self.tag])


    def get_all(self, keyfilter=None):
        '''
        Get all record by key filter.
        Filter is supported as SQL LIKE filter 
        '''
        if not keyfilter:
            keyfilter = '%'
        return self.__sql2array__('SELECT * FROM `'+self.table+'` WHERE `key` LIKE %s and `tag`=%s', [keyfilter, self.tag])
    
    def items(self, keyfilter=None):
        '''
        Get all key-value pair by key filter.
        Filter is supported as SQL LIKE filter 
        '''
        objs = self.get_all(keyfilter)
        return [(obj['key'],self.__getval__(obj)) for obj in objs]
    
    def keys(self, keyfilter=None):
        '''
        Get all key of KVDB by key filter.
        Filter is supported as SQL LIKE filter 
        '''
        objs = self.get_all(keyfilter)
        return [obj['key'] for obj in objs]
    
    def values(self, keyfilter=None):
        '''
        Get all value of KVDB by key filter.
        Filter is supported as SQL LIKE filter 
        '''
        objs = self.get_all(keyfilter)
        return [self.__getval__(obj) for obj in objs]
    
import unittest
class SinKVDBTest(unittest.TestCase):
    '''
    Test Case of SinKVDB
    '''
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        if not hasattr(SinKVDBTest, 'kvdb'):
            con = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
            SinKVDBTest.kvdb = SinKVDB(dbcon=con, table='tb_mykvdb', tag='test', reset=False, debug=True)
        self.kvdb = SinKVDBTest.kvdb
        self.predict = {}   # Hold key-values witch will be tested.
        self.predict['bool'] = True
        self.predict['str'] = 'this is string'
        self.predict['int'] = 1
        self.predict['float'] = 3.14159
        self.predict['dict'] = {'name':'Robin', 'age':23}
        self.predict['list'] = [1,2,3,'str1','str2']
        self.predict['tuple'] = (1,2,3,'abc','def')
        
    def test0Store(self):
        for (k,v) in self.predict.items():
            self.kvdb[k] = v
            
        self.kvdb.commit()
    
    def test1Read(self):
        for k in self.predict.keys():
            self.kvdb[k]
            
    def test2Comp(self):
        self.assertEquals(self.kvdb['bool'], self.predict['bool'], 'bool not eq')
        self.assertEquals(self.kvdb['int'], self.predict['int'], 'int not eq')
        self.assertEquals(self.kvdb['str'], self.predict['str'], 'str not eq')
        self.assertEquals(self.kvdb['float'], self.predict['float'], 'float not eq')
        self.assertDictEqual(self.kvdb['dict'], self.predict['dict'], 'dict not eq')
        self.assertListEqual(self.kvdb['list'], self.predict['list'], 'list not eq')
        self.assertTupleEqual(tuple(self.kvdb['tuple']), self.predict['tuple'], 'tuple not eq')
        
    def test3Items(self):
        # get all key-value pair witch key contain 'i'
        for (k,v) in self.kvdb.items('%i%'):
            print 'key:%s\tval:%s'%(k,v)

    def test3KeysValues(self):
        # get all key-value pair witch key contain 'i'
        print self.kvdb.keys()
        print self.kvdb.values()

    def test5Del(self):
        del self.kvdb['bool']
        del self.kvdb['int']
        del self.kvdb['str']
        del self.kvdb['float']
        del self.kvdb['dict']
        del self.kvdb['list']
        del self.kvdb['tuple']
        
        self.kvdb.commit()
        
        self.assertEquals(self.kvdb['bool'], None, 'bool not deleted')
        self.assertEquals(self.kvdb['int'], None, 'int not deleted')
        self.assertEquals(self.kvdb['str'], None, 'str not deleted')
        self.assertEquals(self.kvdb['float'], None, 'float not deleted')
        self.assertEquals(self.kvdb['dict'], None, 'dict not deleted')
        self.assertEquals(self.kvdb['list'], None, 'list not deleted')
        self.assertEquals(self.kvdb['tuple'], None, 'tuple not deleted')


if __name__ == '__main__':
    unittest.main()

    
    
