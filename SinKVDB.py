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
    def __init__(self, dbcon=None, table=None, tag=None, reset=False, debug = False):
        self.dbcon = dbcon
        self.table = table
        self.tag = tag
        self.debug = debug
        self.__pingdb__()
        if reset:
            self.reset()
        else:
            self.__create_table__()
        
    
    def __delete_table__(self):
        return self.__execsql__(__TPL_DELETETABLE__%self.table)
    
    def __create_table__(self):
        return self.__execsql__(__TPL_CREATETABLE__%self.table)
        
    def __pingdb__(self):
        self.dbcon.ping(True)
        self.dbcur = self.dbcon.cursor()
        
    def __execsql__(self, query, args=None):
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
    
    def __sql2array__(self, sql, args=None):
        count = self.__execsql__(sql, args)
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
        return self.__execsql__('INSERT INTO `'+self.table+'`(`key`, `value`, `tag`, `type`, `createtime`, `modifytime`) VALUES(%s, %s, %s, %s, %s, %s)', [key, value, self.tag, ctype, time.time(), time.time()])
    
    def set_one(self, key, value, ctype):
        return self.__execsql__('UPDATE `'+self.table+'` SET `value`=%s, `tag`=%s, `type`=%s, `modifytime`=%s WHERE `key`=%s', [value, self.tag, ctype, time.time(), key])
    
    def get_one(self, key):
        objs = self.__sql2array__('SELECT * FROM `'+self.table+'` WHERE `key`=%s and `tag`=%s LIMIT 1', [key, self.tag])
        if objs and len(objs):
            return objs[0]
        else:
            return None
    
    def reset(self):
        try:
            self.__delete_table__()
            self.__create_table__()
            return True
        except:
            return False
    
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

import unittest
class SinKVDBTest(unittest.TestCase):
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        con = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
        self.kvdb = SinKVDB(dbcon=con, table='trb', tag='test', reset=True)
        
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
        self.kvdb.dbcon.commit()
    
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
        
if __name__ == '__main__':
    unittest.main()

    
    
