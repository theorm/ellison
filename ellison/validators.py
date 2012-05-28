def is_list_or_tuple(obj, length = None, min_length = None, obj_type = None):
	'''
	>>> is_list_or_tuple([1,2])
	>>> is_list_or_tuple((1,2))
	>>> is_list_or_tuple('asdf')
	Traceback (most recent call last):
		...
	AssertionError: asdf is expected to be a list or tuple, but it is <type 'str'>
	
	>>> is_list_or_tuple([1,2],length=2)
	>>> is_list_or_tuple([1,2],length=3)
	Traceback (most recent call last):
		...
	AssertionError: [1, 2] has length of 2, but is expected to be 3
	
	>>> is_list_or_tuple(['asdf',u'moo'], obj_type = basestring)
	>>> is_list_or_tuple([1,u'moo'], obj_type = basestring)
	Traceback (most recent call last):
		...
	AssertionError: one of the entries in [1, u'moo'] is not of type <type 'basestring'>
	
	>>> is_list_or_tuple(['boo',u'moo'], obj_type = basestring, length = 'asdf')
	Traceback (most recent call last):
		...
	AssertionError: length argument (asdf) should be integer, but it is <type 'str'>
	'''
	assert isinstance(obj, (list,tuple)), '%s is expected to be a list or tuple, but it is %s' % (obj, obj.__class__)

	if length is not None:
		assert isinstance(length, int), 'length argument (%s) should be integer, but it is %s' % (length, length.__class__)
		assert len(obj) == length, '%s has length of %s, but is expected to be %s' % (obj, len(obj), length)
	if min_length is not None:
		assert isinstance(min_length, int), 'min_length argument (%s) should be integer, but it is %s' % (min_length, min_length.__class__)
		assert len(obj) >= min_length, '%s has length of %s, but is expected to be at least %s' % (obj, len(obj), min_length)
		
	if obj_type is not None:
		assert reduce(lambda x,y: x and isinstance(y,obj_type), obj, True), 'one of the entries in %s is not of type %s' % (obj,obj_type)

def is_instance(obj, obj_type):
	assert isinstance(obj,obj_type), '%s is expected to be of type %s but it is %s' % (obj, obj_type, obj.__class__)


if __name__ == "__main__":
	import doctest
	doctest.testmod()