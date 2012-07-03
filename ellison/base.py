from ellison import validators
from copy import copy
from pymongo.son_manipulator import SONManipulator
import logging
import inspect, functools

log = logging.getLogger('ellison')

global _classes_registry
_classes_registry = {}

__all__ = ['ClassInjectorManipulator','Document','Builder','query','Repository','UnitOfWork','DataContext','DataContextInjector','lazy']

class ClassInjectorManipulator(SONManipulator):
    
    def transform_incoming(self, son, collection):
        '''to the DB'''
        if isinstance(son,Document):
            kls = son.__class__.__name__
            son = dict(son)
            son['_cls'] = kls
        for k,v in son.items():
            if isinstance(v,(Document,dict)):
                son[k] = self.transform_incoming(v, collection)
            elif isinstance(v,list):
                for idx,entry in enumerate(v):
                    if isinstance(entry,(Document,dict)):
                        son[k][idx] = self.transform_incoming(entry, collection)
        return son
        
    def transform_outgoing(self, son, collection):
        '''from the DB'''
        if '_cls' in son:
            klass = Document.get_class(son['_cls'])
            son = klass(son)
        for k,v in son.items():
            if isinstance(v,dict):
                if '_cls' in v:
                    klass = Document.get_class(v['_cls'])
                    son[k] = v = klass(son[k])
                son[k] = self.transform_outgoing(v, collection)
            elif isinstance(v,list):
                for idx,entry in enumerate(v):
                    if isinstance(entry,dict):
                        if '_cls' in entry:
                            klass = Document.get_class(entry['_cls'])
                            son[k][idx] = v[idx] = klass(son[k][idx])
                        son[k][idx] = self.transform_outgoing(entry, collection)                
        return son

class DataContextInjector(SONManipulator):
    def __init__(self,data_context):
        self._data_context = data_context
        
    def _inject (self,son):
        if isinstance(son,Document):
            son.__data_context__ = self._data_context
        return son
        
    def transform_incoming(self, son, collection):
        'to DB'
        return self._inject(son)
        
    def transform_outgoing(self, son, collection):
        'from DB'
        return self._inject(son)        

class DocumentMetaclass(type):
    ''' Metaclass for :class:`Document`, responsible for registering child classes'''

    def __new__(cls, name, bases, attrs):
        cls_obj = super(DocumentMetaclass, cls).__new__(cls, name, bases, attrs)
        global _classes_registry
        _classes_registry[name] = cls_obj
        return cls_obj


class Document(dict):
    '''Base class for database documents. Not that documents are expected to be read only.'''
    __metaclass__ = DocumentMetaclass

    @classmethod
    def get_class(self, name):
        if name in _classes_registry:
            return _classes_registry[name]
        else:
            return None

    @classmethod
    def get_registered_subclassess(self):
        ''' Get registered subclasses of :param: `klass` class. '''
        return [k for k in _classes_registry.values() if issubclass(k, self)]

    @classmethod        
    def instantiate_document(self, source):
        if '_cls' in source:
            klass = self.get_class(source['_cls'])
            if klass:
                return klass(source)
            else:
                return eval(source['_cls'])(source)             
        else:
            return None
            
    def __assign_items(self, target, key, source, instantiate = False):
        if '_cls' in source and instantiate:
            target[key] = self.instantiate_document(source)
        else:
            for k,v in source.items():
                if key not in target:
                    target[key] = {}
                if isinstance(v,dict) and not isinstance(v,Document):
                    self.__assign_items(target[key],k,v, instantiate)                   
                else:
                    target[key][k] = v

    def __init__(self, content=None, instantiate_children=False):
        '''
        Document content can be provided in 'content' argument.
        '''

        if content:
            if '_cls' in content and content['_cls'] != self.__class__.__name__:
                raise InvalidDocumentException('Trying to put a wrong type of dict (%s) into this one (%s)' %\
                    (content['_cls'],self.__class__.__name__))
            for k,v in content.items():
                if k != '_cls':
                    if isinstance(v,dict) and not isinstance(v,Document):
                        self.__assign_items(self,k, v, instantiate_children)
                    else:
                        self[k] = v
                else:
                    self['_cls'] = v

    @property
    def mongo_id(self):
        return self.get('_id',None)

class Builder(object):
    '''
    Base class for documents builders. It can ensure the structure of the document,
    assign default values and perform validation of the fields.
    
    Example::
    
        class UserBuilder(Builder):
            structure = {
                'username'      : (True,basestring),
                'first_name'    : (False,basestring),
                'last_name'     : (False,basestring),
                'registered'    : (True,basestring,datetime.datetime.utcnow)
            }
            
        builder = UserBuilder(username='john')
        builder.first_name = 'John'
        builder.last_name = 'Doe'
        builder.build()
    '''
    
    structure = {}
    '''
    Declare the structure of the document here.
    Example::
        
        structure = {
            'name'      : (True,basestring),
            'updated'   : (True,basestring,datetime.datetime.utcnow),
            'phones'    : (False,lambda x: isinstance(x,list) and len(x) <=4)
        }
        
    The key of the structure is the name of the field. The value is a tuple with 2 or 3 elements:
        * First element is the `required` flag: ``True`` - the field is required, ``False`` - is optional.
        * Second element is the validation rule. It could be a type like ``basestring`` or ``int`` or
            a function that returns ``True`` if the field is valid or ``False`` if invalid.
        * The third optional argument is a default value of the field which is set if the `required` field
            is not set explicitly. If the value is a function, then it is executed.
    '''

    object_tag = None
    ''' Set self.object_tag to ``None`` if no tagging is needed (default).
        To tag an object, set to a tuple with key name and value.
        Example::
        
            self.object_tag = ('_cls','Country')
    ''' 
    
    def _get_structure(self):
        m = self.__class__.__mro__
        classes = list(reversed(m[:m.index(Builder)]))
        
        structure = {}
        for cl in classes:
            if hasattr(cl,'structure'):
                structure.update(cl.structure)
        return structure        

    def __init__(self, **kwargs):
        object.__setattr__(self,'structure',self._get_structure())
        object.__setattr__(self,'_document',{})
        for k,v in kwargs.items():
            setattr(self, k, v)
            
    def _is_field_required(self, field):
        return self.structure.get(field,(False,))[0]

    def _validate_field(self, field, value):
        field_structure = self.structure.get(field,(False,))
        if len(field_structure) > 1:
            validator = field_structure[1]
            if isinstance(validator,type) or isinstance(validator,tuple):
                validators.is_instance(value, validator)
            elif validator is not None:
                if validator(value) == False:
                    raise AssertionError('Validation of "%s" failed: "%s"' % (field,value))
                
    def _has_default_value(self, field):
        return len(self.structure.get(field,())) > 2
        
    def _get_default_value(self, field):
        val = self.structure.get(field,())[2]
        try:
            return val()
        except:
            return copy(val)
            
    def __setattr__(self,name,value):
        # XXX Should we allow setting existing attributes on a live builder object? 
        # Now I think we should not to keep builder objects attributes immutable. All except
        # self._document.
        # if name in self.__dict__:
        #   try:
        #       return object.__setattr__(self,name,value)
        #   except AttributeError:
        #       pass
        assert name in self.structure, 'Field "%s" is not in %s structure' % (name,self.__class__)
        try:
            self._validate_field(name, value)
        except AssertionError, e:
            e.args = ('"%s": %s' % (name, e.args[0]),)
            raise e
        self._document[name] = value        
        
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except AttributeError,err:
            error = err
            
        if name not in object.__getattribute__(self,'_document') and object.__getattribute__(self,'_has_default_value')(name):
            setattr(self, name, object.__getattribute__(self,'_get_default_value')(name))
        if name in object.__getattribute__(self,'_document'):
            return object.__getattribute__(self,'_document')[name]
        else:
            raise error
            
    def __iter__(self):
        return self._document.__iter__()
        
    def __getitem__(self,key):
        return self.__getattribute__(key)
        
    def __setitem__(self,key,value):
        return self.__setattr__(key,value)
        
    def __delitem__(self,key):
        return self._document.__delitem__(key)
        
    def __repr__(self):
        return self._document.__repr__()
        
    def get(self,key,default=None):
        return self._document.get(key,default)
    
    def build(self):
        required_fields = [k for k,v in self.structure.items() if v[0]]
        for field in required_fields:
            if field not in self._document and self._has_default_value(field):
                setattr(self, field, self._get_default_value(field))
        for field in required_fields:
            assert field in self._document, 'Field "%s" is required, but not present in %s' % (field, self._document)
        
        doc = self._document.copy()
        
        # build embedded documents
        for k,v in doc.items():
            if isinstance(v,Builder):
                doc[k] = v.build()
        
        if self.object_tag:
            tag,value = self.object_tag
            assert tag not in self._document, \
                '"%s" key is used as object tag, but it has been added to the document: %s = %s' % (tag, tag, self._document[tag])
            doc[tag] = value
            
        return doc

def query(index=None, one=False, sort=None, distinct=None):
    '''
    A decorator that adds syntactic sugar to query methods in a :class:`Repository`.
    The following code::
    
        @query(index=[('first_name',ASCENDING)],sort='last_name')
        def get_users_by_name(self,name,fields=['first_name','last_name']):
            return {
                'first_name'    : name
            }

    Could also be written as::

        def get_users_by_name(self,name,fields=['first_name','last_name']):
            query = self.new_query()
            query['first_name'] = name
            self.collection().ensure_index([('first_name',ASCENDING)])
            return self.collection().find(query,fields=fields).sort('last_name')

    The ``fields`` parameter of the decorated method is optional. If it is provided, `_id` and `_cls` fields
    are always returned.
    
    All of the following parameters are optional:

    :param index: An index to ensure before running the query. Since this is method is for
        queries, there is no need to ensure unique indexes. Writing ``index=[('something',ASCENDING)]``
        is similar to ``collection.ensure_index(['something',ASCENDING])`` in pymongo.
    :param one: If ``True``, will query only one item (similar to ``collection.find_one(...)``). Will
        do ``collection.find(...)`` if ``False`` (default).
    :param sort: Appends ``.sort(...)`` to the cursor. If ``sort`` is a tuple, then the first argument
        is the field to sort on and the second one is ordering (``sort=('name',ASCENDING)``). Otherwise
        a field name is expected.
    :param distinct: Does a ``collection.find(...).distinct(...)`` query. Needs a field name.

    :return: The :class:`~pymongo.cursor.Cursor` instance.
    '''
    def decorator(target):
        def wrapper(self, *args, **kwargs):
            query = target(self, *args, **kwargs)
            
            if query is None:
                return None
                
            query.update(self.new_query())
                        
            if index:
                self.collection().ensure_index(index)
                
            if kwargs.get('fields',None) is not None:
                if '_cls' not in kwargs['fields']:
                    kwargs['fields'].append('_cls')
                    
                fields = kwargs['fields']   
            else:
                fields = None
            
            if one:
                cursor = self.collection().find_one(query, fields=fields)
            else:
                cursor = self.collection().find(query, fields=fields)
                
            if sort is not None:
                if isinstance(sort,tuple):
                    cursor = cursor.sort(sort[0],sort[1])
                else:
                    cursor = cursor.sort(sort)                  
            
            if distinct is not None:
                cursor = cursor.distinct(distinct)
                
            return cursor

        return wrapper

    return decorator

class Repository(object):

    def __init__(self,db):
        assert hasattr(self,'collection_name'), 'Repository class should be extended to include "collection_name" attribute.'
        self._db = db

    def new_query(self):
        return {}

    def collection(self):
        return self.db()[self.collection_name]

    def db(self):
        return self._db

    def add(self, obj):
        if isinstance(obj,Builder):
            doc = obj.build()
        else:
            doc = obj
        doc['_id'] = self.collection().save(doc,safe=True)
        doc = self.collection().database._fix_outgoing(doc,self.collection())
        return doc
        
    def update(self, document):
        assert '_id' in document, 'Trying to update a document without "_id"'
        self.collection().save(document,safe=True)

    def foreach(self,fn, batch_size = 100):
        n = 0
        while True:
            destinations = self.get_all().skip(n).limit(batch_size)
            if n >= destinations.count():
                break
            for dst in destinations:
                fn(dst)
                n += 1
            log.debug("%s: %s entries iterated" % (self.__class__.__name__,n))
        return n

    @query()
    def get_all(self):
        return {}

class DataContext(object):
    'A context that contains repositories.'
    pass
    
class LazyLoadingException(Exception):
    pass

def lazy(method):
    @functools.wraps(method)
    def cache(self,*args,**kwargs):
        if not hasattr(self,'__lazy__'):
            self.__lazy__ = {}
        if method.__name__ not in self.__lazy__:
            self.__lazy__[method.__name__] = functools.partial(method,self=self,data_context=self.__data_context__)(*args,**kwargs)
        return self.__lazy__[method.__name__]
    return cache
    
class UnitOfWork(object):
    def __init__(self,data_context,**kwargs):
        assert isinstance(data_context,DataContext)
        self.data_context = data_context
        for (k,v) in kwargs.items():
            setattr(self,k,v)
            
    def execute(self):
        return None
