# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import unittest
import urllib
from google.appengine.api import urlfetch, memcache
from google.appengine.ext import ndb
import webapp2
from webapp2_extras import i18n
from gaebusiness import gaeutil
from gaebusiness.business import CommandExecutionException
from gaebusiness.gaeutil import UrlFetchCommand, TaskQueueCommand, ModelSearchCommand, SingleModelSearchCommand, \
    NaiveSaveCommand, NaiveUpdateCommand, FindOrCreateModelCommand, SaveCommand, UpdateCommand
from gaeforms.ndb.form import ModelForm
from mock import Mock
from util import GAETestCase


class UrlfecthTests(unittest.TestCase):
    def test_https_post(self):
        params = {'id': 'foo', 'token': 'bar'}
        url = 'https://foo.bar.com/rest'
        rpc = Mock()
        result = Mock()
        result.status_code = 200
        result.content = '{"ticket":"123456"}'
        rpc.get_result = Mock(return_value=result)
        gaeutil.urlfetch.create_rpc = Mock(return_value=rpc)
        fetch = Mock()
        gaeutil.urlfetch.make_fetch_call = fetch
        command = UrlFetchCommand(url, params, urlfetch.POST)
        command.execute()
        self.assertEqual(result, command.result)
        fetch.assert_called_once_with(rpc, url, urllib.urlencode(params), method=urlfetch.POST,
                                      validate_certificate=True, headers={})

    def test_http_get(self):
        params = {'id': 'foo', 'token': 'bar'}
        url = 'http://foo.bar.com/rest'
        rpc = Mock()
        result = Mock()
        result.status_code = 200
        result.content = '{"ticket":"123456"}'
        rpc.get_result = Mock(return_value=result)
        gaeutil.urlfetch.create_rpc = Mock(return_value=rpc)
        fetch = Mock()
        gaeutil.urlfetch.make_fetch_call = fetch
        command = UrlFetchCommand(url, params, validate_certificate=False)
        command.execute()
        self.assertEqual(result, command.result)
        fetch.assert_called_once_with(rpc, '%s?%s' % (url, urllib.urlencode(params)), None, method=urlfetch.GET,
                                      validate_certificate=False, headers={})


class TaskQueueTests(unittest.TestCase):
    def test_queue_creation(self):
        task_obj = Mock()
        task_cls = Mock(return_value=task_obj)
        rpc_mock = Mock()
        queue_obj = Mock()
        queue_cls = Mock(return_value=queue_obj)
        gaeutil.Queue = queue_cls
        gaeutil.taskqueue.create_rpc = Mock(return_value=rpc_mock)
        gaeutil.Task = task_cls
        queue_name = 'foo'
        params = {'param1': 'bar'}
        url = '/mytask'
        cmd = TaskQueueCommand(queue_name, url, params=params)
        cmd.execute()
        task_cls.assert_called_once_with(url=url, params=params)
        queue_obj.add_async.assert_called_once_with(task_obj, rpc=rpc_mock)
        rpc_mock.get_result.assert_called_once_with()


# Stub used for next tests
class SomeModel(ndb.Model):
    index = ndb.IntegerProperty()

    @classmethod
    def query_index_ordered(cls):
        return cls.query().order(cls.index)


class ModelSearchCommandTests(GAETestCase):
    def _assert_result(self, cmd, begin, end):
        self.assertListEqual(list(xrange(begin, end)), [some_model.index for some_model in cmd.result])

    def test_cache_key(self):
        key = ModelSearchCommand(SomeModel.query_index_ordered(), 3)._cache_key()
        key_offset = ModelSearchCommand(SomeModel.query_index_ordered(), 3, offset=2)._cache_key()
        key_other_query = ModelSearchCommand(SomeModel.query_index_ordered().filter(SomeModel.index == 1), 3,
                                             offset=2)._cache_key()
        key_other_query2 = ModelSearchCommand(SomeModel.query_index_ordered().filter(SomeModel.index == 2), 3,
                                              offset=2)._cache_key()
        self.assertNotEqual(key, key_offset)
        self.assertNotEqual(key_offset, key_other_query)
        self.assertNotEqual(key_other_query2, key_other_query)

    def test_cache(self):
        ndb.put_multi([SomeModel(index=i) for i in xrange(7)])
        # asserting the first results are not cached
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cache_begin=False)
        self.assertIsNone(memcache.get(cmd._cache_key()))

        # asserting the first results are cached
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3)
        cursor = cmd.execute().cursor
        cached = memcache.get(cmd._cache_key())
        self.assertIsNotNone(cached)
        cached_model_keys, cached_cursor = cached
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3)), [some_model.index for some_model in cached_models])

        # asserting nothing is cached when using arg use_cache=False
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor, use_cache=False)
        cursor2 = cmd.execute().cursor
        self.assertIsNone(memcache.get(cmd._cache_key()))

        # asserting items are cached
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor, cache_begin=False).execute()
        cached_model_keys, cached_cursor = memcache.get(cmd._cache_key())
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cached_models])
        self.assertEqual(cursor2, cached_cursor)

        # asserting cached with offset
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, offset=3).execute()
        cached_model_keys, cached_cursor = memcache.get(cmd._cache_key())
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cached_models])
        self.assertEqual(cursor2, cached_cursor)

        # asserting cached is used
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        self.assertIsNone(cmd._ModelSearchCommand__future)
        self.assertIsNotNone(cmd._ModelSearchCommand__cached_keys)
        self._assert_result(cmd, 3, 6)


        # asserting results are not cached if len of results are less then search page_size
        ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor2).execute()
        self.assertIsNone(memcache.get(cursor2.urlsafe()))


    def test_offset_search(self):
        ndb.put_multi([SomeModel(index=i) for i in xrange(10)])
        search = ModelSearchCommand(SomeModel.query_index_ordered(), 2, offset=1).execute()
        self._assert_result(search, 1, 3)
        # offset and cursor
        search = ModelSearchCommand(SomeModel.query_index_ordered(), 2, search.cursor, offset=2).execute()
        self._assert_result(search, 5, 7)


    def test_cursor_search(self):
        ndb.put_multi([SomeModel(index=i) for i in xrange(8)])

        # Asserting cursor works
        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3).execute()
        self.assertTrue(search.more)
        cursor = search.cursor
        self.assertIsNotNone(cursor)
        self._assert_result(search, 0, 3)

        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        self.assertTrue(search.more)
        cursor2 = search.cursor
        self.assertIsNotNone(cursor2)
        self._assert_result(search, 3, 6)

        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor2.urlsafe()).execute()
        cursor3 = search.cursor
        self.assertIsNotNone(cursor2)
        self._assert_result(search, 6, 8)


class SingleModelSearchTests(GAETestCase):
    def test_no_model_on_db(self):
        cmd = SingleModelSearchCommand(SomeModel.query())
        result = cmd()
        self.assertIsNone(result)
        self.assertIsNone(memcache.get(cmd._cache_key()))

    def test_model_on_db(self):
        model = SomeModel()
        model.put()
        cmd = SingleModelSearchCommand(SomeModel.query())
        result = cmd()
        # is a list because SingleModel inherits from ModelSearchCommand
        self.assertListEqual([model.key], memcache.get(cmd._cache_key())[0])
        self.assertEqual(model, result)


class NaiveSaveCommandTests(GAETestCase):
    def test_save(self):
        cmd = NaiveSaveCommand(SomeModel, {'index': 10})
        result = cmd()
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.key)
        self.assertEqual(10, result.index)


class ModelStub(ndb.Model):
    name = ndb.StringProperty(required=True)
    age = ndb.IntegerProperty(required=True)


class NaiveUpdateCommandTests(GAETestCase):
    def assert_update(self, id):
        model = ModelStub(id=1, name='a', age=1)
        model.put()
        properties = {'name': 'b', 'age': 2}
        cmd = NaiveUpdateCommand(ModelStub, id, properties)
        result = cmd()
        model_on_db = model.key.get()
        self.assertIsNotNone(result)
        self.assertEqual(result, model_on_db)
        self.assertDictEqual(properties, model_on_db.to_dict())

    def test_update_with_numeric_id(self):
        self.assert_update(1)

    def test_update_with_string_id(self):
        self.assert_update('1')

    def test_update_with_key(self):
        self.assert_update(ndb.Key(ModelStub, 1))


class FindOrCreateModelCommandTests(GAETestCase):
    def test_success(self):
        properties = {'name': 'b', 'age': 2}
        cmd = FindOrCreateModelCommand(ModelStub.query(), ModelStub, properties)
        result = cmd()
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.key)
        self.assertDictEqual(properties, result.to_dict())
        properties2 = {'name': 'c', 'age': 3}
        cmd = FindOrCreateModelCommand(ModelStub.query(), ModelStub, properties2)
        result2 = cmd()
        self.assertEqual(result, result2)
        self.assertDictEqual(properties, result2.to_dict())


class ModelStubForm(ModelForm):
    _model_class = ModelStub


class SaveModelStubCommand(SaveCommand):
    _model_form_class = ModelStubForm

# workaround for test using internationalization
app = webapp2.WSGIApplication(
    [webapp2.Route('/', None, name='upload_handler')])

request = webapp2.Request({'SERVER_NAME': 'test', 'SERVER_PORT': 80,
                           'wsgi.url_scheme': 'http'})
request.app = app
app.set_globals(app=app, request=request)

i18n.default_config['default_timezone'] = 'America/Sao_Paulo'
# end of workaround


class SaveCommandTests(GAETestCase):
    def test_init(self):
        self.assertRaises(Exception, SaveCommand)

    def _assert_validation_errors(self, cmd, expected_error_keys):
        self.assertRaises(CommandExecutionException, cmd)
        self.assertIsNone(cmd.result)
        self.assertSetEqual(expected_error_keys, set(cmd.errors.iterkeys()))

    def test_validation(self):
        expected_error_keys = set('name,age'.split(','))
        cmd = SaveModelStubCommand()
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set(['name'])
        cmd = SaveModelStubCommand(age='31')
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set(['age'])
        cmd = SaveModelStubCommand(age='not a number', name='foo')
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set(['age'])
        cmd = SaveModelStubCommand(name='foo')
        self._assert_validation_errors(cmd, expected_error_keys)

    def test_success(self):
        cmd = SaveModelStubCommand(name='foo', age='31')
        result = cmd()
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.key)
        model_on_db = result.key.get()
        self.assertEqual('foo', model_on_db.name)
        self.assertEqual(31, model_on_db.age)


class UpdateModelStubCommand(UpdateCommand):
    _model_form_class = ModelStubForm


class UpdateCommandTests(GAETestCase):
    def test_init(self):
        self.assertRaises(Exception, UpdateCommand, 1)

    def _assert_validation_errors(self, cmd, expected_error_keys):
        self.assertRaises(CommandExecutionException, cmd)
        self.assertIsNone(cmd.result)
        self.assertSetEqual(expected_error_keys, set(cmd.errors.iterkeys()))

    def test_validation(self):
        expected_error_keys = set('model,name,age'.split(','))
        model_key = ndb.Key(ModelStub, 1)
        cmd = UpdateModelStubCommand(model_key)
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set('model,name'.split(','))
        cmd = UpdateModelStubCommand(model_key, age='31')
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set('model,age'.split(','))
        cmd = UpdateModelStubCommand(model_key, age='not a number', name='foo')
        self._assert_validation_errors(cmd, expected_error_keys)

        expected_error_keys = set('model,age'.split(','))
        cmd = UpdateModelStubCommand(model_key, name='foo')
        self._assert_validation_errors(cmd, expected_error_keys)

    def test_success(self):
        old_properties = {'name': 'old_foo', 'age': 26}
        model_key = ModelStub(**old_properties).put()
        cmd = UpdateModelStubCommand(model_key, name='foo', age='31')
        result = cmd()
        self.assertIsNotNone(result)
        self.assertEqual(model_key, result.key)
        self.assertDictEqual(old_properties, cmd.old_model_properties)
        model_on_db = result.key.get()
        self.assertEqual('foo', model_on_db.name)
        self.assertEqual(31, model_on_db.age)






