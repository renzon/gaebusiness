# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import unittest
import urllib
from google.appengine.api import urlfetch, memcache
from google.appengine.ext import ndb
from gaebusiness import gaeutil
from gaebusiness.gaeutil import UrlFetchCommand, TaskQueueCommand, ModelSearchCommand
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
    def test_cache(self):
        ndb.put_multi([SomeModel(index=i) for i in xrange(7)])
        #asserting nothing is cached for the first results
        cursor = ModelSearchCommand(SomeModel.query_index_ordered(), 3).execute().cursor
        self.assertIsNone(memcache.get(cursor.urlsafe()))

        #asserting nothing is cached when using arg use_cache=False
        cursor2 = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor, use_cache=False).execute().cursor
        self.assertIsNone(memcache.get(cursor.urlsafe()))

        #asserting items are cached
        ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        cached_model_keys, cached_cursor = memcache.get(cursor.urlsafe())
        cached_models = ndb.get_multi(cached_model_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cached_models])
        self.assertEqual(cursor2, cached_cursor)

        #asserting cached is used
        cmd = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        self.assertIsNone(cmd._ModelSearchCommand__future)
        self.assertIsNotNone(cmd._ModelSearchCommand__cached_keys)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in cmd.result])


        #asserting results are not cached if len of results are less then search page_size
        ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor2).execute()
        self.assertIsNone(memcache.get(cursor2.urlsafe()))


    def test_cursor_search(self):
        ndb.put_multi([SomeModel(index=i) for i in xrange(8)])

        #Asserting cursor works
        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3).execute()
        self.assertTrue(search.more)
        cursor = search.cursor
        self.assertIsNotNone(cursor)
        self.assertListEqual(list(xrange(3)), [some_model.index for some_model in search.result])

        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor).execute()
        self.assertTrue(search.more)
        cursor2 = search.cursor
        self.assertIsNotNone(cursor2)
        self.assertListEqual(list(xrange(3, 6)), [some_model.index for some_model in search.result])

        search = ModelSearchCommand(SomeModel.query_index_ordered(), 3, cursor2.urlsafe()).execute()
        cursor3 = search.cursor
        self.assertIsNotNone(cursor2)
        self.assertListEqual(list(xrange(6, 8)), [some_model.index for some_model in search.result])

