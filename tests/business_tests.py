# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from itertools import izip
import unittest
from google.appengine.ext import ndb
from gaebusiness.business import Command, CommandParallel, CommandExecutionException, CommandSequential, CommandListBase
from gaebusiness.gaeutil import DeleteCommand
from gaeutil_tests import ModelStub
from mock import Mock
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
    def test_chaining_methods(self):
        self.assertEqual('foo', CommandMock('foo').execute().result.ppt)
        self.assertEqual('bar', CommandMock('bar')().ppt)

    def test_not_commiting_on_error(self):
        cmd = CommandMock('foo', True)
        self.assertRaises(CommandExecutionException, cmd)
        self.assertIsNone(cmd.commit())

    def test_update_errors(self):
        cmd = Command()
        self.assertDictEqual({}, cmd.errors)
        errors = {'foo': 'foomsg'}
        cmd.update_errors(**errors)
        self.assertDictEqual(errors, cmd.errors)
        errors['bar'] = 'barmsg'
        cmd.update_errors(**errors)
        self.assertDictEqual(errors, cmd.errors)


class CommandBaseListTest(GAETestCase):
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


class CommandListTests(unittest.TestCase):
    def test_append(self):
        cmd_list = CommandListBase()
        cmd = Command()
        cmd_list.append(cmd)
        self.assertEqual(cmd, cmd_list[0])

    def test_extend(self):
        cmd_list = CommandListBase()
        cmds = [Command() for i in xrange(3)]
        cmd_list.extend(cmds)
        for cmd, cmd_on_list in izip(cmds, cmd_list):
            self.assertEqual(cmd, cmd_on_list)

    def test_len(self):
        cmd = CommandListBase()
        self.assertEqual(0, len(cmd))
        cmd.append(Command())
        self.assertEqual(1, len(cmd))

    def test_bool(self):
        cmd = CommandListBase()
        self.assertFalse(bool(cmd))
        cmd.append(Command())
        self.assertTrue(bool(cmd))


class CommandParallelTests(CommandBaseListTest):
    def test_empty(self):
        CommandParallel()()

    def test_update_error_when_command_execution_exception_is_raised(self):
        class RaiseCommand(Command):
            def do_business(self):
                self.add_error('key', 'msg')
                raise CommandExecutionException()

        cmd = CommandParallel(RaiseCommand())
        self.assertRaises(CommandExecutionException, cmd)
        self.assertDictEqual({'key': 'msg'}, cmd.errors)

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
        self.assertEqual(mock_2.result, command_list.result, 'result must be equals to last command result')
        self.assertEqual(result, command_list.result)


    def test_execute_with_error(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_0 = CommandMock(MOCK_0)
        mock_1 = CommandMock(MOCK_1, ERROR_KEY, ERROR_MSG)
        mock_2 = CommandMock(MOCK_2)

        command_list = CommandParallel(mock_0, mock_1, mock_2)
        self.assertRaises(CommandExecutionException, command_list.execute)
        for cmd, m in izip(command_list, [MOCK_0, MOCK_1, MOCK_2]):
            self.assert_command_only_commit_not_executed(cmd, m)
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)


    def test_execute_errors_msgs(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        command_list = CommandParallel(CommandMock(MOCK_0, ERROR_KEY, ERROR_MSG),
                                       CommandMock(MOCK_1, ANOTHER_ERROR_KEY, ANOTHER_ERROR_MSG),
                                       CommandMock(MOCK_2))
        self.assertRaises(CommandExecutionException, command_list.execute)
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


class CommandSequentialTests(CommandBaseListTest):
    def test_empty(self):
        CommandSequential()()

    def test_execute_successful_business(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = CommandSequential(mock_1, mock_2)
        errors = command_list.execute().errors
        self.assert_command_executed(mock_1, MOCK_1)
        self.assert_command_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)

    def test_execute_call(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        mock_1 = CommandMock(MOCK_1)
        mock_2 = CommandMock(MOCK_2)
        command_list = CommandSequential(mock_1, mock_2)
        result = command_list()
        errors = command_list.errors
        self.assert_command_executed(mock_1, MOCK_1)
        self.assert_command_executed(mock_2, MOCK_2)
        self.assertDictEqual({}, errors)
        self.assertIsNotNone(result)
        self.assertEqual(mock_2.result, command_list.result, 'result must be equals to last command result')
        self.assertEqual(result, command_list.result)


    def test_execute_business_with_error(self):
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        command_list = CommandSequential(CommandMock(MOCK_1, ERROR_KEY, ERROR_MSG), CommandMock(MOCK_2))
        self.assertRaises(CommandExecutionException, command_list.execute)
        self.assert_command_only_commit_not_executed(command_list[0], MOCK_1)
        self.assert_command_not_executed(command_list[1])
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)

    def test_execute_errors_msgs(self):
        MOCK_0 = "mock 0"
        MOCK_1 = "mock 1"
        MOCK_2 = "mock 2"
        command_list = CommandSequential(CommandMock(MOCK_0, ERROR_KEY, ERROR_MSG),
                                         CommandMock(MOCK_1, ANOTHER_ERROR_KEY, ANOTHER_ERROR_MSG),
                                         CommandMock(MOCK_2))
        self.assertRaises(CommandExecutionException, command_list.execute)
        self.assert_command_only_commit_not_executed(command_list[0], MOCK_0)
        for cmd, m in izip(command_list[1:], [MOCK_1, MOCK_2]):
            self.assert_command_not_executed(cmd)
        self.assertDictEqual({ERROR_KEY: ERROR_MSG}, command_list.errors)


    def test_commit(self):
        class CommadSequentialMock(CommandSequential):
            def __init__(self, ):
                super(CommadSequentialMock, self).__init__(Command())
                self._to_commit = ModelMock()

        cmd = CommadSequentialMock()
        cmd()
        self.assertIsNotNone(ModelMock.query().get())


def cmd_with_handle_previous_mocked():
    c = Command()
    c.handle_previous = Mock()
    return c


class HandlePreviousTests(unittest.TestCase):
    def assert_handle_previous_not_called(self, cmd):
        self.assertFalse(cmd.handle_previous.called)


    def assert_handler_previous_called_once(self, previous, current):
        current.handle_previous.assert_called_once_with(previous)

    def test_calls_on_sequential(self):
        sequential_cmds = CommandSequential(*[cmd_with_handle_previous_mocked() for i in range(3)])
        sequential_cmds()
        self.assert_handle_previous_not_called(sequential_cmds[0])
        self.assert_handler_previous_called_once(sequential_cmds[0], sequential_cmds[1])

    def test_calls_on_parallel(self):
        parallel_cmds = CommandParallel(*[cmd_with_handle_previous_mocked() for i in range(3)])
        parallel_cmds()
        for cmd in parallel_cmds:
            self.assert_handle_previous_not_called(cmd)

    def test_mixing_commands_on_sequential(self):
        sequential_cmds = CommandSequential(*[cmd_with_handle_previous_mocked() for i in range(3)])
        parallel_cmds = CommandParallel(*[cmd_with_handle_previous_mocked() for i in range(3)])
        cmd = cmd_with_handle_previous_mocked()

        CommandSequential(*[cmd, parallel_cmds, sequential_cmds]).execute()

        self.assert_handle_previous_not_called(cmd)
        self.assert_handler_previous_called_once(cmd, parallel_cmds[0])
        self.assert_handler_previous_called_once(cmd, parallel_cmds[1])
        self.assert_handler_previous_called_once(cmd, parallel_cmds[2])
        self.assert_handler_previous_called_once(parallel_cmds, sequential_cmds[0])
        self.assert_handler_previous_called_once(sequential_cmds[0], sequential_cmds[1])
        self.assert_handler_previous_called_once(sequential_cmds[1], sequential_cmds[2])

    def test_mixing_commands_on_parallel(self):
        sequential_cmds = CommandSequential(*[cmd_with_handle_previous_mocked() for i in range(3)])
        parallel_cmds = CommandParallel(*[cmd_with_handle_previous_mocked() for i in range(3)])
        cmd = cmd_with_handle_previous_mocked()

        CommandParallel(*[sequential_cmds, parallel_cmds, cmd]).execute()

        self.assert_handle_previous_not_called(sequential_cmds[0])
        self.assert_handler_previous_called_once(sequential_cmds[0], sequential_cmds[1])
        self.assert_handler_previous_called_once(sequential_cmds[1], sequential_cmds[2])
        for cmd in parallel_cmds:
            self.assert_handle_previous_not_called(cmd)

        self.assert_handle_previous_not_called(cmd)


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
