# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from google.appengine.ext import ndb
from gaebusiness.business import Command, CommandList, CommandExecutionException
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
    def __init__(self, model_ppt, business_error=False):
        super(CommandMock, self).__init__()
        self.business_error = business_error
        self.set_up_executed = False
        self.commit_executed = False
        self._model_ppt = model_ppt
        self.result = None

    def set_up(self):
        if self.business_error:
            self.add_error(ERROR_KEY, ERROR_MSG)
        self.set_up_executed = True

    def do_business(self, stop_on_error=False):
        self.result = ModelMock(ppt=self._model_ppt)

    def commit(self):
        return self.result


class CommandMockWithErrorOnBusiness(CommandMock):
    def do_business(self, stop_on_error=False):
        self.add_error(ANOTHER_ERROR_KEY, ANOTHER_ERROR_MSG)


class BusinessTests(GAETestCase):
    def test__add__(self):
        mock0 = CommandMock('')
        mock1 = CommandMock('')
        mock2 = CommandMock('')
        cmdlist = mock0 + mock1 + mock2
        self.assertIs(mock0, cmdlist[0])
        self.assertIs(mock1, cmdlist[1])
        self.assertIs(mock2, cmdlist[2])

    def test_execute_chaining(self):
        self.assertEqual('foo', CommandMock('foo').execute().result.ppt)


    def assert_usecase_executed(self, usecase, model_ppt):
        self.assertTrue(usecase.set_up_executed)
        self.assertEqual(model_ppt, usecase.result.ppt, "do_business not executed")
        self.assertIsNotNone(usecase.result.key, "result should be saved on db")

    def assert_usecase_only_commit_not_executed(self, usecase, model_ppt):
        self.assertTrue(usecase.set_up_executed)
        self.assertEqual(model_ppt, usecase.result.ppt, "do_business not executed")
        self.assertIsNone(usecase.result.key, "result should not be saved on db")


    def assert_usecase_not_executed(self, usecase):
        self.assertTrue(usecase.set_up_executed)
        self.assertIsNone(usecase.result)


    def test_execute_successful_business(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = mock_1 + mock_2
        errors = command_list.execute().errors
        self.assert_usecase_executed(mock_1, MOCK_1)
        self.assert_usecase_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)

    def test_execute_call(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = mock_1 + mock_2
        result = command_list()
        errors = command_list.errors
        self.assert_usecase_executed(mock_1, MOCK_1)
        self.assert_usecase_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)
        self.assertIsNotNone(result)
        self.assertEqual(result, command_list.result)

    def test_explicit_main_command(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"

        class CommandListComposition(CommandList):
            def __init__(self, label, label_1):
                main_command = CommandMock(label)
                CommandList.__init__(self, main_command + CommandMock(label_1), main_command)

        command_list = CommandListComposition(MOCK_0, MOCK_1)
        errors = command_list.execute().errors
        self.assert_usecase_executed(command_list[0], MOCK_0)
        self.assert_usecase_executed(command_list[1], MOCK_1)
        self.assertEqual(MOCK_0, command_list.result.ppt)
        self.assertDictEqual({}, errors)

    def test_implicit_main_command(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"

        class CommandListComposition(CommandList):
            def __init__(self, label, label_1):
                CommandList.__init__(self, CommandMock(label) + CommandMock(label_1))

        command_list = CommandListComposition(MOCK_0, MOCK_1)
        errors = command_list.execute().errors
        self.assert_usecase_executed(command_list[0], MOCK_0)
        self.assert_usecase_executed(command_list[1], MOCK_1)
        self.assertEqual(MOCK_1, command_list.result.ppt)  # it takes the last command as de default main command
        self.assertDictEqual({}, errors)

    def test_business_composition(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"

        class CommandListComposition(CommandList):
            def __init__(self, label, label_1):
                CommandList.__init__(self, CommandMock(label) + CommandMock(label_1))

        command_list = CommandListComposition(MOCK_0, MOCK_1)
        errors = command_list.execute().errors
        self.assert_usecase_executed(command_list[0], MOCK_0)
        self.assert_usecase_executed(command_list[1], MOCK_1)
        self.assertDictEqual({}, errors)


    def test_execute_business_not_stopping_on_error(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_0 = CommandMockWithErrorOnBusiness(MOCK_0)
        mock_1 = CommandMock(MOCK_1, True)
        mock_2 = CommandMock(MOCK_2)

        command_list = mock_0 + mock_1 + mock_2
        self.assertRaises(CommandExecutionException, command_list.execute, False)
        self.assert_usecase_not_executed(mock_0)
        self.assert_usecase_not_executed(mock_1)
        self.assert_usecase_only_commit_not_executed(mock_2, MOCK_2)
        self.assertDictEqual({ERROR_KEY: ERROR_MSG, ANOTHER_ERROR_KEY: ANOTHER_ERROR_MSG}, command_list.errors)

    def test_execute_business_stopping_on_error(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        commands = [CommandMock(MOCK_1, True), CommandMock(MOCK_2)]
        command_list = CommandList(commands)
        self.assertRaises(CommandExecutionException, command_list.execute, True)
        self.assert_usecase_not_executed(commands[0])
        self.assert_usecase_not_executed(commands[1])
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)

    def test_execute_business_stopping_on_error_in_method_on_business(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        commands = [CommandMockWithErrorOnBusiness(MOCK_0), CommandMock(MOCK_1, True), CommandMock(MOCK_2)]
        command_list = CommandList(commands)
        self.assertRaises(CommandExecutionException, command_list.execute, True)
        for cmd in command_list:
            self.assert_usecase_not_executed(cmd)
        self.assertDictEqual({ANOTHER_ERROR_KEY: ANOTHER_ERROR_MSG}, command_list.errors)


    def test_commit(self):
        class CommadListMock(CommandList):
            def __init__(self, ):
                super(CommadListMock, self).__init__([Command()])
                self._to_commit = ModelMock()

        cmd = CommadListMock()
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
