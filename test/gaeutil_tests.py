# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import unittest
import urllib
from google.appengine.api import urlfetch, memcache
from google.appengine.ext import ndb
from gaebusiness import gaeutil
from gaebusiness.gaeutil import UrlFetchCommand, TaskQueueCommand, ModelSearchCommand, SingleModelSearchCommand
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

        #asserting the first results are cached
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3)
        cursor = cmd.execute().cursor
        cached = memcache.get(cmd._cache_key())
        self.assertIsNotNone(cached)
        cached_model_keys, cached_cursor = cached
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3)), [some_model.index for some_model in cached_models])

        #asserting nothing is cached when using arg use_cache=False
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor, use_cache=False)
        cursor2 = cmd.execute().cursor
        self.assertIsNone(memcache.get(cmd._cache_key()))

        #asserting items are cached
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor, cache_begin=False).execute()
        cached_model_keys, cached_cursor = memcache.get(cmd._cache_key())
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cached_models])
        self.assertEqual(cursor2, cached_cursor)

        #asserting cached with offset
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, offset=3).execute()
        cached_model_keys, cached_cursor = memcache.get(cmd._cache_key())
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cached_models])
        self.assertEqual(cursor2, cached_cursor)

        #asserting cached is used
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        self.assertIsNone(cmd._ModelSearchCommand__future)
        self.assertIsNotNone(cmd._ModelSearchCommand__cached_keys)
        self._assert_result(cmd, 3, 6)


        #asserting results are not cached if len of results are less then search page_size
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