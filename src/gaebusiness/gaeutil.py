# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import urllib
from google.appengine.api import urlfetch, taskqueue, memcache
from google.appengine.api.taskqueue import Task
from google.appengine.api.taskqueue import Queue
from google.appengine.ext import ndb
from google.appengine.ext.ndb.query import Cursor
from gaebusiness.business import Command


class UrlFetchCommand(Command):
    def __init__(self, url, params={}, method=urlfetch.GET, headers={}, validate_certificate=True, deadline=30,
                 **kwargs):
        super(UrlFetchCommand, self).__init__(**kwargs)
        self.method = method
        self.headers = headers
        self.validate_certificate = validate_certificate
        self.url = url
        self.params = None
        self.deadline = deadline
        if params:
            encoded_params = urllib.urlencode(params)
            if method in (urlfetch.POST, urlfetch.PUT, urlfetch.PATCH):
                self.params = encoded_params
            else:
                self.url = "%s?%s" % (url, encoded_params)

    def set_up(self):
        self._rpc = urlfetch.create_rpc(deadline=self.deadline)
        urlfetch.make_fetch_call(self._rpc, self.url, self.params, method=self.method,
                                 validate_certificate=self.validate_certificate, headers=self.headers)

    def do_business(self, stop_on_error=False):
        self.result = self._rpc.get_result()


class TaskQueueCommand(Command):
    def __init__(self, queue_name, url, **kwargs):
        '''
        kwargs are the same used on Task class
        (https://developers.google.com/appengine/docs/python/taskqueue/tasks#Task)
        '''
        super(TaskQueueCommand, self).__init__()
        self._task = Task(url=url, **kwargs)
        self._queue_name = queue_name


    def set_up(self):
        self._rpc = taskqueue.create_rpc()
        q = Queue(self._queue_name)
        q.add_async(self._task, rpc=self._rpc)

    def do_business(self, stop_on_error=False):
        self._rpc.get_result()


class ModelSearchCommand(Command):
    def __init__(self, query, page_size=100, start_cursor=None, use_cache=True, **kwargs):
        self.__future = None
        self.__cached_keys = None
        self.cursor = None
        self.more = None
        if isinstance(start_cursor, basestring):
            start_cursor = Cursor(urlsafe=start_cursor)
        super(ModelSearchCommand, self).__init__(query=query, page_size=page_size, start_cursor=start_cursor,
                                                 use_cache=use_cache,
                                                 **kwargs)

    def set_up(self):
        if self.use_cache and self.start_cursor:
            try:
                cached_tuple = memcache.get(self.start_cursor.urlsafe())
                if cached_tuple:
                    self.__cached_keys, self.cursor, self.more = cached_tuple[0], cached_tuple[1], True
            except:
                pass
        if not self.__cached_keys:
            self.__future = self.query.fetch_page_async(self.page_size, start_cursor=self.start_cursor, keys_only=True)

    def do_business(self, stop_on_error=True):
        if self.__future:
            model_keys, self.cursor, self.more = self.__future.get_result()
            future = ndb.get_multi_async(model_keys)
            if self.use_cache and self.start_cursor and len(model_keys) == self.page_size:
                memcache.set(self.start_cursor.urlsafe(), (model_keys, self.cursor))
            self.result = [f.get_result() for f in future]
        else:
            self.result = ndb.get_multi(self.__cached_keys)


