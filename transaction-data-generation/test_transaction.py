import asyncio
import unittest
import utils
from unittest.mock import patch
import datetime
import time
import numpy as np
import user_balance
import message_transaction
import call_transaction
import data_transaction
from asgiref.sync import async_to_sync

dropDBSQL = "DROP DATABASE IF EXISTS testdb"
createDBSQL = "CREATE DATABASE IF NOT EXISTS testdb"
useDBSQL = "USE testdb"
logFile = 'transactionDummy.log'


class TestFunctions(unittest.TestCase):

    def test_random_id_generator(self):
        self.assertEqual(len(utils.random_id_generator(24)), 24)
        self.assertTrue(utils.random_id_generator(24).isalnum())

    def test_get_current_time(self):
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(
            ts).strftime('%Y-%m-%d %H:%M:%S')
        self.assertAlmostEqual(utils.get_current_time(), timestamp)

    def test_select_user_from_user_balance(self):
        user_id = 'abc123'
        self.assertEqual(str(utils.select_user_from_user_balance(user_id)).strip(
        ), "select * from user_balance where id = '{}'".format(user_id))

    def test_pick_users_from_user_balance(self):
        offset = 0
        number_of_users = 10
        self.assertEqual(str(utils.pick_users_from_user_balance(offset, number_of_users)).strip(
        ), "select id from user_balance limit {},{}".format(offset, number_of_users))

    def test_select_id_from_transaction_type(self):
        self.assertEqual(str(utils.select_id_from_transaction_type()).strip(
        ), "select id from transaction_type")

    def test_size_of_user_balance_table(self):
        self.assertEqual(str(utils.size_of_user_balance_table()).strip(
        ), "select count(*) from user_balance")

    def test_list_of_tuples_to_list(self):
        list_of_tuples = [(1,), (2,)]
        self.assertEqual(utils.list_of_tuples_to_list(list_of_tuples), [1, 2])

    def test_sub(self):
        self.assertEqual(utils.sub(5, 2), 3)

    @patch('utils.random.randint')
    @patch('utils.np.random.poisson')
    def test_random_distrubution_generator_small_scale(self, mockedpoisson, mockedrandint):
        mockedpoisson.return_value = [9, 10, 11, 12]
        mockedrandint.return_value = 10
        size = 4
        scale = 20
        expected = [190, 210, 230, 250]
        self.assertEqual(
            utils.random_distrubution_generator(size, scale), expected)

    @patch('utils.random.getrandbits')
    @patch('utils.np.random.poisson')
    def test_random_distrubution_generator_large_scale(self, mockedpoisson, mockedrandbits):
        mockedpoisson.return_value = [9, 10, 11, 12]
        mockedrandbits.return_value = 1237653
        size = 4
        scale = 2**31 + 10
        expected = np.int64([190, 210, 230, 250]).all()
        self.assertEqual(utils.random_distrubution_generator(
            size, scale).all(), expected)

    @patch('utils.random_id_generator')
    @patch('utils.random_distrubution_generator')
    def test_user_balance_generator(self, mockedrdg, mockedrig):
        mockedrdg.return_value = [100, 200]
        mockedrig.return_value = 'abc123'
        self.assertEqual(utils.user_balance_generator(2), [['abc123', 'abc123'], [
                         100, 200], [100, 200], [100, 200], [100, 200]])

    def test_get_create_database_consumption(self):
        self.assertEqual(str(utils.get_create_database_consumption()).strip(
        ), "CREATE DATABASE IF NOT EXISTS consumption")

    def test_get_use_database_consumption(self):
        self.assertEqual(
            str(utils.get_use_database_consumption()).strip(), "USE consumption")

    def test_get_create_table_message(self):
        self.assertEqual(str(utils.get_create_table_message()).strip().replace(" ", ""), """
            CREATE TABLE IF NOT EXISTS message (
                id varchar(32) primary key,
                plan_id varchar(32),
                sender_id varchar(32),
                receiver_id varchar(32),
                sender_mobile varchar(15),
                receiver_mobile varchar(15),
                num_of_messages int,
                amount_charged double,
                created_at timestamp,
                FOREIGN KEY (sender_id) REFERENCES user_balance(id),
                FOREIGN KEY (receiver_id) REFERENCES user_balance(id)
            )
        """.strip().replace(" ", ""))

    def test_get_log_file(self):
        self.assertEqual(utils.get_log_file(), 'transaction1.log')

    def test_get_create_table_user_balance(self):
        self.assertEqual(str(utils.get_create_table_user_balance()).strip().replace(" ", ""), """CREATE TABLE IF NOT EXISTS user_balance (
            id varchar(32) primary key,
            data bigint,
            ott int,
            calls int,
            messages int
        )""".strip().replace(" ", ""))

    def test_get_create_table_call(self):
        self.assertEqual(str(utils.get_create_table_call()).strip().replace(" ", ""), """CREATE TABLE IF NOT EXISTS `call` (
            id varchar(32) primary key,
            plan_id varchar(32),
            caller_id varchar(32),
            receiver_id varchar(32),
            caller_mobile varchar(15),
            receiver_mobile varchar(15),
            num_of_seconds int,
            amount_charged double,
            created_at timestamp,
            FOREIGN KEY (caller_id) REFERENCES user_balance(id),
            FOREIGN KEY (receiver_id) REFERENCES user_balance(id)
        )""".strip().replace(" ", ""))

    def test_get_create_table_data(self):
        self.assertEqual(str(utils.get_create_table_data()).strip().replace(" ", ""), """CREATE TABLE IF NOT EXISTS data (
            id varchar(32) primary key,
            plan_id varchar(32),
            user_id varchar(32),
            kb_consumed int,
            amount_charged double,
            created_at timestamp,
            FOREIGN KEY (user_id) REFERENCES user_balance(id)
        )""".strip().replace(" ", ""))

    def test_pseudo_drop(self):
        self.assertEqual(str(utils.pseudodrop()).strip(), "")

    @patch('user_balance.utils.get_use_database_consumption')
    @patch('user_balance.utils.get_create_database_consumption')
    @patch('user_balance.utils.get_create_table_user_balance')
    @patch('user_balance.utils.pseudodrop')
    def test_user_balance(self, mockdrop, mocktablecreate, mockcreate, mockuse):
        mockdrop.return_value = dropDBSQL
        mocktablecreate.return_value = """CREATE TABLE IF NOT EXISTS user_balance (
            id varchar(32) primary key,
            data bigint,
            ott int,
            calls int,
            messages int
        )"""
        mockcreate.return_value = createDBSQL
        mockuse.return_value = useDBSQL
        self.assertEqual(user_balance.user_balance(), True)

    @patch('message_transaction.utils.get_log_file')
    @patch('message_transaction.utils.get_use_database_consumption')
    @patch('message_transaction.utils.select_user_from_user_balance')
    @patch('message_transaction.utils.random.randint')
    @patch('message_transaction.utils.list_of_tuples_to_list')
    @patch('message_transaction.utils.pick_users_from_user_balance')
    @patch('message_transaction.utils.sub')
    @patch('message_transaction.utils.size_of_user_balance_table')
    @patch('message_transaction.utils.get_create_database_consumption')
    @patch('message_transaction.utils.get_create_table_message')
    @patch('message_transaction.utils.random_number_generator')
    @patch('message_transaction.utils.pseudodrop')
    def test_message_transaction_calls(self, mockdrop, mockgenerate, mocktablecreate, mockcreate, mocksizeofuserbalance, mocksub, mockpickusers, mocktolist, mockrandint, mockselectuser, mockuse, mockgetlog):
        mockdrop.return_value = dropDBSQL
        mocktablecreate.return_value = """CREATE TABLE IF NOT EXISTS message (
            id varchar(32) primary key,
            plan_id varchar(32),
            sender_id varchar(32),
            receiver_id varchar(32),
            sender_mobile varchar(15),
            receiver_mobile varchar(15),
            num_of_messages int,
            amount_charged double,
            created_at timestamp
        )"""
        mockgetlog.return_value = logFile
        mockcreate.return_value = createDBSQL
        mockuse.return_value = useDBSQL
        mocksizeofuserbalance.return_value = ""
        mocksub.return_value = -1
        mockpickusers.return_value = ""
        mocktolist.return_value = [
            '041bkwmt364m0d1yxnpgop3r', '0jxmithmmhqpmz98kp5wter3']
        mockrandint.return_value = 2
        mockgenerate.return_value = "+91 5235645874"
        mockselectuser.return_value = ""
        loop = asyncio.get_event_loop()
        var = loop.run_until_complete(
            message_transaction.message_transaction())
        self.assertEqual(var, True)

    @patch('call_transaction.utils.get_log_file')
    @patch('call_transaction.utils.get_use_database_consumption')
    @patch('call_transaction.utils.select_user_from_user_balance')
    @patch('call_transaction.utils.random.randint')
    @patch('call_transaction.utils.list_of_tuples_to_list')
    @patch('call_transaction.utils.pick_users_from_user_balance')
    @patch('call_transaction.utils.sub')
    @patch('call_transaction.utils.size_of_user_balance_table')
    @patch('call_transaction.utils.get_create_database_consumption')
    @patch('call_transaction.utils.get_create_table_call')
    @patch('call_transaction.utils.random_number_generator')
    @patch('call_transaction.utils.pseudodrop')
    def test_call_transaction_calls(self, mockdrop, mockgenerate, mocktablecreate, mockcreate, mocksizeofuserbalance, mocksub, mockpickusers, mocktolist, mockrandint, mockselectuser, mockuse, mockgetlog):
        mockdrop.return_value = dropDBSQL
        mocktablecreate.return_value = """CREATE TABLE IF NOT EXISTS `call` (
            id varchar(32) primary key,
            plan_id varchar(32),
            caller_id varchar(32),
            receiver_id varchar(32),
            caller_mobile varchar(15),
            receiver_mobile varchar(15),
            num_of_seconds int,
            amount_charged double,
            created_at timestamp
        )"""
        mockgetlog.return_value = logFile
        mockcreate.return_value = createDBSQL
        mockuse.return_value = useDBSQL
        mocksizeofuserbalance.return_value = ""
        mocksub.return_value = -1
        mockpickusers.return_value = ""
        mocktolist.return_value = [
            '041bkwmt364m0d1yxnpgop3r', '0jxmithmmhqpmz98kp5wter3']
        mockrandint.return_value = 2
        mockgenerate.return_value = "+91 5235645874"
        mockselectuser.return_value = ""
        loop = asyncio.get_event_loop()
        var = loop.run_until_complete(call_transaction.call_transaction())
        self.assertEqual(var, True)

    @patch('data_transaction.utils.get_log_file')
    @patch('data_transaction.utils.get_use_database_consumption')
    @patch('data_transaction.utils.select_user_from_user_balance')
    @patch('data_transaction.utils.random.randint')
    @patch('data_transaction.utils.list_of_tuples_to_list')
    @patch('data_transaction.utils.pick_users_from_user_balance')
    @patch('data_transaction.utils.sub')
    @patch('data_transaction.utils.size_of_user_balance_table')
    @patch('data_transaction.utils.get_create_database_consumption')
    @patch('data_transaction.utils.get_create_table_data')
    @patch('data_transaction.utils.pseudodrop')
    def test_data_transaction_calls(self, mockdrop, mocktablecreate, mockcreate, mocksizeofuserbalance, mocksub, mockpickusers, mocktolist, mockrandint, mockselectuser, mockuse, mockgetlog):
        mockdrop.return_value = dropDBSQL
        mocktablecreate.return_value = """CREATE TABLE IF NOT EXISTS data (
            id varchar(32) primary key,
            plan_id varchar(32),
            user_id varchar(32),
            kb_consumed int,
            amount_charged double,
            created_at timestamp
        )"""
        mockgetlog.return_value = logFile
        mockcreate.return_value = createDBSQL
        mockuse.return_value = useDBSQL
        mocksizeofuserbalance.return_value = ""
        mocksub.return_value = -1
        mockpickusers.return_value = ""
        mocktolist.return_value = [
            '041bkwmt364m0d1yxnpgop3r', '0jxmithmmhqpmz98kp5wter3']
        mockrandint.return_value = 0
        mockselectuser.return_value = ""
        loop = asyncio.get_event_loop()
        var = loop.run_until_complete(data_transaction.data_transaction())
        self.assertEqual(var, True)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
