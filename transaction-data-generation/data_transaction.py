import logging
import requests
import utils
from time import sleep
import credentials
from mysql.connector import Error
import random
import utils
import asyncio
from asgiref.sync import sync_to_async
import dbconnection

logger = logging.getLogger()


def debug_logger(msg):
    logger.debug(msg)


def error_logger(msg):  # pragma: no cover
    logger.error(msg)


def warning_logger(msg):
    logger.warning(msg)


async def insert_execute(*args):
    insert_statement = """
                    INSERT INTO data(id, plan_id, user_id, kb_consumed, amount_charged, created_at) values (
                            '{}',
                            '{}',
                            '{}',
                            {},
                            {},
                            '{}'
                        )
                    """.format(*args)
    try:
        cursor.execute(insert_statement)
        connectionData.commit()
        await sync_to_async(debug_logger)("Data transaction inserted")
        await sync_to_async(warning_logger)(str(cursor.rowcount)+" rows affected")
    except Error as e:  # pragma: no cover
        await sync_to_async(error_logger)(e)


async def data_transaction():
    logging.basicConfig(filename=utils.BASE_PATH + utils.get_log_file(),
                        filemode='w',
                        format='%(asctime)s, %(name)s %(threadName)s %(message)s',
                        level=logging.DEBUG)
    try:
        with dbconnection.SqlManager(credentials.connection_details["host"],
                                     credentials.connection_details["user"],
                                     credentials.connection_details["key"]) as connect:
            await sync_to_async(debug_logger)("Connection Established from data transaction")
            global connectionData
            global cursor
            connectionData = connect
            cursor = connectionData.cursor()
            await sync_to_async(debug_logger)("Cursor Fetched")
            # sql statements to use database and tables
            cursor.execute(utils.get_create_database_consumption())
            cursor.execute(utils.get_use_database_consumption())
            cursor.execute(utils.get_create_table_data())
            # fetch userids from user_balance table
            # number of users - number of records to pick
            num_records_user_balance_stmt_data = utils.size_of_user_balance_table()
            cursor.execute(num_records_user_balance_stmt_data)
            num_records_user_balance_data = cursor.fetchone()
            if(num_records_user_balance_data):  # pragma: no cover
                num_records_user_balance_data = num_records_user_balance_data[0]
            offset_range_data = utils.sub(
                num_records_user_balance_data, utils.NUMBER_OF_USERS_PICKED)
            offset_data = 0
            if offset_range_data <= 0:
                offset_data = 0
            else:  # pragma: no cover
                offset_data = random.randint(0, offset_range_data)

            pick_users_statement = utils.pick_users_from_user_balance(
                offset_data, utils.NUMBER_OF_USERS_PICKED)
            cursor.execute(pick_users_statement)
            list_of_tuples = cursor.fetchall()
            user_list = utils.list_of_tuples_to_list(list_of_tuples)

            for _ in range(utils.NO_OF_TRANSACTION):
                random_user_id = random.choice(user_list)
                user_balance_sql = utils.select_user_from_user_balance(
                    random_user_id)
                cursor.execute(user_balance_sql)
                user_balance = cursor.fetchone()
                if(user_balance == None):
                    user_balance = (0, 0, 0, 0, 0)
                kb_consumed = random.randint(
                    0, user_balance[utils.DATA_TYPE_ID + 1])
                timestamp = utils.get_current_time()
                generated_id = utils.random_id_generator(32)
                plan_id = random.choice(utils.data_plan_id_list)
                amount_charged = 0
                data = {
                    "id": random_user_id,
                    "call": 0,
                    "data": kb_consumed,
                    "sms": 0,
                    "ott": 0
                }
                insert_statement = """
                    INSERT INTO data(id, plan_id, user_id, kb_consumed, amount_charged, created_at) values (
                            '{}',
                            '{}',
                            '{}',
                            {},
                            {},
                            '{}'
                        )
                    """.format(generated_id, plan_id, random_user_id, kb_consumed, amount_charged, timestamp)
                try:
                    # cursor.execute(insert_statement)
                    await sync_to_async(debug_logger)("Data transaction inserted")
                    await sync_to_async(warning_logger)(str(cursor.rowcount)+" rows affected")
                except Error as e:  # pragma: no cover
                    await sync_to_async(error_logger)(e)
                    # await insert_execute()
                    # requests.post(utils.POST_REQUEST_URL, data=data)
                    await sync_to_async(debug_logger)("1 Data Transaction inserted")
            # connectionData.commit()
            # cursor.execute(utils.pseudodrop())
        return True
    except Error as e:  # pragma: no cover
        await sync_to_async(error_logger)(e)
        return False


def call_main():  # pragma: no cover
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    var = loop.run_until_complete(data_transaction())
    return var


if __name__ == '__main__':  # pragma: no cover
    call_main()
