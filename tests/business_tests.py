# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from itertools import izip
from google.appengine.ext import ndb
from gaebusiness.business import Command, CommandParallel, CommandExecutionException
from gaebusiness.gaeutil import DeleteCommand
from gaeutil_tests import ModelStub
from mommygae import mommy
from util import GAETestCase


class ModelMock(ndb.Model):
    ppt = ndb.StringProperty()


ERROR_KEY = "error_key"
ERROR_MSG = "TEST_ERROR"

ANOTHER_ERROR_KEY = "another_error_key"
ANOTHER_ERROR_MSG = "ANOTHER_TEST_ERROR"


class CommandMock(Command):
    def __init__(self, model_ppt, error_key=None, error_msg=None):
        super(CommandMock, self).__init__()
        self.error_msg = error_msg
        self.error_key = error_key
        self.set_up_executed = False
        self.business_executed = False
        self.commit_executed = False
        self._model_ppt = model_ppt
        self.result = None

    def set_up(self):
        self.set_up_executed = True

    def do_business(self, stop_on_error=False):
        self._to_commit = ModelMock(ppt=self._model_ppt)
        if self.error_key:
            self.add_error(self.error_key, self.error_msg)
        self.result = self._to_commit
        self.business_executed = True

    def commit(self):
        self.commit_executed = True
        return super(CommandMock, self).commit()


class CommandTests(GAETestCase):
    def test_not_commiting_on_error(self):
        cmd = CommandMock('foo', True)
        self.assertRaises(CommandExecutionException, cmd, False)
        self.assertIsNone(cmd.commit())


class CommandParallelTests(GAETestCase):
    def test_execute_chaining(self):
        self.assertEqual('foo', CommandMock('foo').execute().result.ppt)
        self.assertEqual('bar', CommandMock('bar')().ppt)


    def assert_command_executed(self, command, model_ppt):
        self.assertTrue(command.set_up_executed)
        self.assertTrue(command.business_executed)
        self.assertTrue(command.commit_executed)
        self.assertEqual(model_ppt, command.result.ppt, "do_business not executed")
        self.assertIsNotNone(command.result.key, "result should be saved on db")

    def assert_command_only_commit_not_executed(self, command, model_ppt):
        self.assertTrue(command.set_up_executed)
        self.assertTrue(command.business_executed)
        self.assertFalse(command.commit_executed)
        self.assertEqual(model_ppt, command.result.ppt, "do_business not executed")
        self.assertIsNone(command.result.key, "result should not be saved on db")


    def assert_command_not_executed(self, command):
        self.assertFalse(command.set_up_executed)
        self.assertFalse(command.business_executed)
        self.assertFalse(command.commit_executed)
        self.assertIsNone(command.result)


    def assert_command_only_setup_executed(self, command):
        self.assertTrue(command.set_up_executed)
        self.assertFalse(command.business_executed)
        self.assertFalse(command.commit_executed)
        self.assertIsNone(command.result)


    def test_execute_successful_business(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = CommandParallel(mock_1, mock_2)
        errors = command_list.execute().errors
        self.assert_command_executed(mock_1, MOCK_1)
        self.assert_command_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)

    def test_execute_call(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = CommandParallel(mock_1, mock_2)
        result = command_list()
        errors = command_list.errors
        self.assert_command_executed(mock_1, MOCK_1)
        self.assert_command_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)
        self.assertIsNotNone(result)
        self.assertEqual(result, command_list.result)


    def test_implicit_return_last_command_result(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"

        class CommandParallelComposition(CommandParallel):
            def __init__(self, label, label_1):
                CommandParallel.__init__(self, CommandMock(label), CommandMock(label_1))

        command_list = CommandParallelComposition(MOCK_0, MOCK_1)
        errors = command_list.execute().errors
        self.assert_command_executed(command_list[0], MOCK_0)
        self.assert_command_executed(command_list[1], MOCK_1)
        self.assertEqual(MOCK_1, command_list.result.ppt)  # it takes the last command as de default main command
        self.assertDictEqual({}, errors)

    def test_business_composition(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"

        class CommandParallelComposition(CommandParallel):
            def __init__(self, label, label_1):
                CommandParallel.__init__(self, CommandMock(label), CommandMock(label_1))

        command_list = CommandParallelComposition(MOCK_0, MOCK_1)
        errors = command_list.execute().errors
        self.assert_command_executed(command_list[0], MOCK_0)
        self.assert_command_executed(command_list[1], MOCK_1)
        self.assertDictEqual({}, errors)


    def test_execute_business_not_stopping_on_error(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_0 = CommandMock(MOCK_0)
        mock_1 = CommandMock(MOCK_1, ERROR_KEY, ERROR_MSG)
        mock_2 = CommandMock(MOCK_2)

        command_list = CommandParallel(mock_0, mock_1, mock_2)
        self.assertRaises(CommandExecutionException, command_list.execute, False)
        self.assert_command_only_commit_not_executed(mock_0, MOCK_0)
        self.assert_command_only_commit_not_executed(mock_1, MOCK_1)
        self.assert_command_only_commit_not_executed(mock_2, MOCK_2)
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)

    def test_execute_business_stopping_on_error(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        command_list = CommandParallel(CommandMock(MOCK_1, ERROR_KEY, ERROR_MSG), CommandMock(MOCK_2))
        self.assertRaises(CommandExecutionException, command_list.execute, True)
        self.assert_command_only_commit_not_executed(command_list[0], MOCK_1)
        self.assert_command_only_setup_executed(command_list[1])
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)

    def test_execute_errors_msgs(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        command_list = CommandParallel(CommandMock(MOCK_0, ERROR_KEY, ERROR_MSG),
                                       CommandMock(MOCK_1, ANOTHER_ERROR_KEY, ANOTHER_ERROR_MSG),
                                       CommandMock(MOCK_2))
        self.assertRaises(CommandExecutionException, command_list.execute, False)
        for cmd, m in izip(command_list, [MOCK_0, MOCK_1, MOCK_2]):
            self.assert_command_only_commit_not_executed(cmd, m)
        self.assertDictEqual({ANOTHER_ERROR_KEY: ANOTHER_ERROR_MSG, ERROR_KEY: ERROR_MSG}, command_list.errors)


    def test_commit(self):
        class CommadParallelMock(CommandParallel):
            def __init__(self, ):
                super(CommadParallelMock, self).__init__(Command())
                self._to_commit = ModelMock()

        cmd = CommadParallelMock()
        cmd()
        self.assertIsNotNone(ModelMock.query().get())


class DeleteCommnadTests(GAETestCase):
    def test_delete(self):
        model = mommy.save_one(ModelStub)
        self.assertIsNotNone(model.key.get())
        DeleteCommand(model.key).execute()
        self.assertIsNone(model.key.get())

        models = [mommy.save_one(ModelStub) for i in range(3)]
        model_keys = [m.key for m in models]
        self.assertListEqual(models, ndb.get_multi(model_keys))
        DeleteCommand(*model_keys).execute()
        self.assertListEqual([None, None, None], ndb.get_multi(model_keys))
