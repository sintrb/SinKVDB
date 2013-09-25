# -*- coding: UTF-8 -*
'''
Created on 2013-9-23

@author: RobinTang
'''

import MySQLdb
import time
import json
import types

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
    def __init__(self, dbcon=None, table=None, tag=None, reset=False, debug = False):
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
        self.__pingdb__()
        if reset:
            self.reset_table()
        else:
            self.__create_table__()
        
    
    def __delete_table__(self):
        '''
        Delete the table
        '''
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
    
    def __getitem__(self, key):
        obj = self.get_one(key)
        if obj:
            if obj['type'] == 'json':
                return json.loads(obj['value'])
            else:
                return obj['value']
        else:
            return None
    
    def __setitem__(self, key, value):
        ctype = None
        if type(value) in (types.DictType, types.ListType, types.TupleType):
            ctype = 'json'
            value = json.dumps(value)
        elif type(value) not in (types.NoneType, types.LongType, types.IntType, types.BooleanType, types.FloatType, types.StringType):
            raise Exception('Not support value type: %s'%type(value))
        
        if not self.set_one(key, value, ctype):
            return self.add_one(key, value, ctype)
        else:
            return True
        
    def __delitem__(self, key):
        return self.__execsql__('DELETE FROM `'+self.table+'` WHERE `key`=%s and `tag`=%s LIMIT 1', [key, self.tag])

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
        self.assertEquals(bool(self.kvdb['bool']), bool(self.predict['bool']), 'bool not eq')
        self.assertEquals(self.kvdb['int'], str(self.predict['int']), 'int not eq')
        self.assertEquals(self.kvdb['str'], str(self.predict['str']), 'str not eq')
        self.assertEquals(self.kvdb['float'], str(self.predict['float']), 'float not eq')
        self.assertDictEqual(self.kvdb['dict'], self.predict['dict'], 'dict not eq')
        self.assertListEqual(list(self.kvdb['list']), self.predict['list'], 'list not eq')
        self.assertListEqual(list(self.kvdb['tuple']), list(self.predict['tuple']), 'tuple not eq')
    
    def test3Del(self):
        del self.kvdb['bool']
        del self.kvdb['int']
        del self.kvdb['str']
        del self.kvdb['float']
        del self.kvdb['dict']
        del self.kvdb['list']
        del self.kvdb['tuple']
        
        self.kvdb.commit()
        
        self.assertEquals(self.kvdb['bool'], None, 'bool not eq')
        self.assertEquals(self.kvdb['int'], None, 'int not eq')
        self.assertEquals(self.kvdb['str'], None, 'str not eq')
        self.assertEquals(self.kvdb['float'], None, 'float not eq')
        self.assertEquals(self.kvdb['dict'], None, 'dict not eq')
        self.assertEquals(self.kvdb['list'], None, 'list not eq')
        self.assertEquals(self.kvdb['tuple'], None, 'tuple not eq')
    
if __name__ == '__main__':
    unittest.main()
    SinKVDB(1, 1, 1, 1, 1)

    
    
