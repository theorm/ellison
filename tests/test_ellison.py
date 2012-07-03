from ellison import *
from pymongo import *
import unittest
from pymongo.son_manipulator import ObjectIdInjector
from datetime import datetime

_db = Connection().test
_data_context = DataContext()

_db.add_son_manipulator(ObjectIdInjector())
_db.add_son_manipulator(ClassInjectorManipulator())
_db.add_son_manipulator(DataContextInjector(_data_context))

class TestRepository(Repository):
	collection_name = 'ellison'

	@query()
	def get_all(self):
		return {}
		
	@query(index=[('a',DESCENDING)])
	def get_all_with_index(self,fields=None):
		return {}

	@query(index='a')
	def get_all_with_index_default(self,fields=None):
		return {}

	@query(one=True)
	def get_one_by_a(self,a):
		return {
			'a' : a
		}

	@query(sort=('b',DESCENDING))
	def get_all_by_a_sort_by_b_desc(self,a):
		return {
			'a' : a
		}

	@query(sort='b')
	def get_all_by_a_sort_by_b_default(self,a):
		return {
			'a' : a
		}
		
	@query(sort='b',distinct='c')
	def get_all_by_a_distinct_c(self,a):
		return {
			'a' : a
		}
	
	@query()
	def get_wrong_1(self):
		return None

	@query()
	def get_wrong_2(self,fields=234):
		return 3
		
	@query(index=23)
	def get_wrong_3(self):
		return 3
		
	@query()
	def find_similar(self,other):
	    if 'a' in other:
	        return {
	            'a' : other.get('a'),
	            '_id' : {
	                '$ne' : other.get('_id'),
	            }
	        }
	    else:
	        return None
		
class TestDocumentBuilder(Builder):
	structure = {
		'a'		: (True,basestring),
		'b'		: (False,int),
		'c'		: (True,float,42.0)
	}    
	
		
class QueryDecoratorTest(unittest.TestCase):
	
	def setUp(self):
		self.repository = TestRepository(_db)
		for a,b in [('a',1),('a',2),('b',1),('b',2),('b',3),('c',1)]:
			self.repository.add(TestDocumentBuilder(a=a,b=b))
		
		self.repository.add(TestDocumentBuilder(a='a',b=1,c=0.5))
		self.repository.add(TestDocumentBuilder(a='a',b=1,c=25.1))
		
		self.real_number_of_objects = self.repository.collection().find().count()
				
	def tearDown(self):
		self.repository.collection().drop()	    
		
	def test_default_and_indexes(self):
		
		self.assertEquals(self.real_number_of_objects, self.repository.get_all().count())
		self.assertEquals(self.real_number_of_objects, self.repository.get_all_with_index().count())
		self.assertEquals(self.real_number_of_objects, self.repository.get_all_with_index_default().count())
		
	def test_fields(self):
		objects = list(self.repository.get_all_with_index(fields=['a']).limit(2))
		self.assertEquals(2, len(objects))
		self.assertEquals([u'a',u'_id'], objects[0].keys())

		objects = list(self.repository.get_all_with_index(fields=[]).limit(1))
		self.assertEquals([u'_id'], objects[0].keys())

	def test_one(self):
		obj = self.repository.get_one_by_a('a')
		self.assertEquals('a',obj['a'])
		
	def test_sort(self):
		objects = list(self.repository.get_all_by_a_sort_by_b_desc('a'))
		self.assertEquals(4,len(objects))
		self.assertEquals([2,1,1,1], [o['b'] for o in objects])
		
		objects = list(self.repository.get_all_by_a_sort_by_b_default('a'))
		self.assertEquals(4,len(objects))
		self.assertEquals([1,1,1,2], [o['b'] for o in objects])
		
	def test_distinct(self):
		objects = sorted(list(self.repository.get_all_by_a_distinct_c('a')))
		self.assertEquals(3,len(objects))
		self.assertEquals([0.5,25.1,42], objects)
		
	def test_wrong(self):
		self.assertEquals(None,self.repository.get_wrong_1())
		
		try:
			self.repository.get_wrong_2()
			self.fail('Should raise an error')
		except:
			pass

		try:
			self.repository.get_wrong_3()
			self.fail('Should raise an error')
		except:
			pass

class LazyTestDocument(Document):
    @lazy
    def similar(self,data_context):
        return data_context.docs.find_similar(self)
        
    @lazy 
    def alike(self,data_context,something_else):
        assert something_else == 1
        return data_context.docs.find_similar(self)
        
    @lazy
    def hash(self,data_context):
        return datetime.now()

class LazyTestDocumentBuilder(TestDocumentBuilder):
    object_tag = ('_cls','LazyTestDocument')


class TestLazyLoading(unittest.TestCase):
    def setUp(self):
        _data_context.docs = TestRepository(_db)
    	for a,b in [('a',1),('a',2),('b',1),('b',2),('b',3),('c',1)]:
    		_data_context.docs.add(LazyTestDocumentBuilder(a=a,b=b))
    
    def test_lazy_loading(self):
        doc = _data_context.docs.get_one_by_a('a')
        self.assertEquals(1,doc.similar().count())
        self.assertEquals(1,doc.alike(something_else=1).count())

        doc1 = _data_context.docs.get_one_by_a('a')
        doc2 = _data_context.docs.get_one_by_a('a')
        self.assertNotEquals(doc1.hash(),doc2.hash())
        
if __name__ == '__main__':
	unittest.main()