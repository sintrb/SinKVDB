# -*- coding: UTF-8 -*
'''
Created on 2013-9-28
Test case for SinKVDB
@author: RobinTang
'''
import MySQLdb
import unittest
from SinKVDB import SinKVDB
class SinKVDBTest(unittest.TestCase):
    '''
    Test Case of SinKVDB
    '''
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        if not hasattr(SinKVDBTest, 'kvdb'):
            con = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
            SinKVDBTest.kvdb = SinKVDB(dbcon=con, table='tb_mykvdb', tag='test', reset=False, debug=False, cache=True, cachesize=4)
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


