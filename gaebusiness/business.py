# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from google.appengine.ext import ndb


class CommandExecutionException(Exception):
    """
    Exception that indicates problems on execution
    """
    pass


def to_model_list(models):
    if models is None:
        return []
    return [models] if isinstance(models, ndb.Model) else models


class Command(object):
    def __init__(self):
        self.errors = {}
        self.result = None
        self._to_commit = None

    def update_errors(self, **errors):
        return self.errors.update(errors)


    def add_error(self, key, msg):
        self.errors[key] = msg

    def set_up(self):
        '''
        Must set_up data for business.
        It should fetch data asyncrounously if needed
        '''
        pass

    def do_business(self, stop_on_error=True):
        '''
        Must do the main business of use case
        '''
        pass

    def commit(self):
        '''
        Must return a Model, or a list of it to be commited on DB
        '''
        if not self.errors:
            return self._to_commit

    def execute(self, stop_on_error=True):
        self.set_up()
        self.do_business(stop_on_error)
        if self.errors:
            raise CommandExecutionException(unicode(self.errors))
        ndb.put_multi(to_model_list(self.commit()))
        return self

    def __call__(self, stop_on_error=True):
        self.execute(stop_on_error)
        return self.result


class CommandListBase(Command):
    def __init__(self, *commands):
        super(CommandListBase, self).__init__()
        self.__commands = list(commands)

    def __getitem__(self, index):
        return self.__commands[index]

    def raise_exception_if_errors(self):
        if self.errors:
            raise CommandExecutionException(unicode(self.errors))


class CommandParallel(CommandListBase):
    def set_up(self):
        for cmd in self:
            cmd.set_up()

    def do_business(self, stop_on_error=True):
        for cmd in self:
            if not cmd.errors:
                cmd.do_business(stop_on_error)
            self.update_errors(**cmd.errors)
            if stop_on_error:
                self.raise_exception_if_errors()
        self.raise_exception_if_errors()
        self.result = self[-1].result

    def commit(self):
        models = to_model_list(super(CommandParallel, self).commit())
        for cmd in self:
            models.extend(to_model_list(cmd.commit()))
        return models


class CommandSequential(CommandListBase):
    def do_business(self, stop_on_error=True):
        if stop_on_error:
            for cmd in self:
                try:
                    cmd()
                except CommandExecutionException, e:
                    self.update_errors(**cmd.errors)
                    raise e
            self.result = self[-1].result
        else:
            for cmd in self:
                try:
                    cmd()
                except CommandExecutionException:
                    self.update_errors(**cmd.errors)
            self.result = self[-1].result
            self.raise_exception_if_errors()

