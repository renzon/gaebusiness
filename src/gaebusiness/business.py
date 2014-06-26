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
    def __init__(self, **kwargs):
        self.errors = {}
        self.result = None
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
        self._to_commit = None

    def __add__(self, other):
        try:
            other.insert(0, self)
            return other
        except AttributeError:
            return CommandList([self, other])

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
        return self._to_commit

    def execute(self, stop_on_error=True):
        self.set_up()
        self.do_business(stop_on_error)
        ndb.put_multi(to_model_list(self.commit()))
        return self

    def __call__(self, stop_on_error=True):
        self.execute(stop_on_error)
        return self.result


class CommandList(Command):
    def __init__(self, commands, main_command=None, **kwargs):
        super(CommandList, self).__init__(**kwargs)
        self.__commands = commands
        self.__main_command = main_command or self[-1]

    def _insert(self, index, element):
        self.__commands.insert(index, element)

    def __add__(self, command):
        if isinstance(command, Command):
            self.__commands.append(command)
        else:
            self.__commands.extend(command.__commands)
        self.__main_command = self[-1]
        return self

    def __getitem__(self, index):
        return self.__commands[index]

    def set_up(self):
        for cmd in self:
            cmd.set_up()

    def do_business(self, stop_on_error=True):
        for cmd in self:
            if not cmd.errors:
                cmd.do_business(stop_on_error)
            self.errors.update(cmd.errors)
            if stop_on_error and self.errors:
                self.result = self.__main_command.result
                raise CommandExecutionException(unicode(self.errors))
        if self.errors:
            raise CommandExecutionException(unicode(self.errors))
        self.result = self.__main_command.result

    def commit(self):
        to_commit = []
        for cmd in self:
            to_commit.extend(to_model_list(cmd.commit()))
        return to_commit
