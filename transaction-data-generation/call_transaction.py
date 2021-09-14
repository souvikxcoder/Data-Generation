import logging
import utils
from time import sleep
import credentials
from mysql.connector import Error
import random
import utils
import asyncio
from asgiref.sync import sync_to_async
import dbconnection
import requests

logger = logging.getLogger()


def debug_logger(msg):
    logger.debug(msg)


def error_logger(msg):
    logger.error(msg)


def warning_logger(msg):  # pragma: no cover
    logger.warning(msg)


async def insert_execute(*args):
    insert_statement = """
                    INSERT INTO `call`(id, plan_id, caller_id, receiver_id, caller_mobile, receiver_mobile,num_of_seconds,amount_charged,created_at) values (
                            '{}',
                            '{}',
                            '{}',
                            '{}',
                            '{}',
                            '{}',
                            {},
                            {},
                            '{}'
                        )
                    """.format(*args)
    try:  # pragma: no cover
        cursor.execute(insert_statement)
        connectionCall.commit()
        await sync_to_async(debug_logger)("Transaction inserted")
        await sync_to_async(warning_logger)(str(cursor.rowcount)+" rows affected")
    except Error as e:
        await sync_to_async(error_logger)(e)


async def call_transaction():
    logging.basicConfig(filename=utils.BASE_PATH + utils.get_log_file(),
                        filemode='w',
                        format='%(asctime)s, %(name)s %(threadName)s %(message)s',
                        level=logging.DEBUG)
    try:
        with dbconnection.SqlManager(credentials.connection_details["host"],
                                     credentials.connection_details["user"],
                                     credentials.connection_details["key"]) as connect:
            await sync_to_async(debug_logger)("Connection Established from call transaction")
            global connectionCall
            global cursor
            connectionCall = connect
            cursor = connectionCall.cursor()
            await sync_to_async(debug_logger)("Cursor Fetched")
            # sql statements to use database and tables
            cursor.execute(utils.get_create_database_consumption())
            cursor.execute(utils.get_use_database_consumption())
            cursor.execute(utils.get_create_table_call())

            # fetch userids from user_balance table
            # number of users - number of records to pick
            num_records_user_balance_stmt_calls = utils.size_of_user_balance_table()
            cursor.execute(num_records_user_balance_stmt_calls)
            num_records_user_balance_calls = cursor.fetchone()
            if(num_records_user_balance_calls):  # pragma: no cover
                num_records_user_balance_calls = num_records_user_balance_calls[0]
            offset_range_calls = utils.sub(
                num_records_user_balance_calls, utils.NUMBER_OF_USERS_PICKED)
            offset_calls = 0
            if offset_range_calls <= 0:
                offset_calls = 0
            else:  # pragma: no cover
                offset_calls = random.randint(0, offset_range_calls)

            pick_users_statement = utils.pick_users_from_user_balance(
                offset_calls, utils.NUMBER_OF_USERS_PICKED)
            cursor.execute(pick_users_statement)
            list_of_tuples = cursor.fetchall()
            user_list = utils.list_of_tuples_to_list(list_of_tuples)
            for _ in range(utils.NO_OF_TRANSACTION):
                # random sender and receiver
                if (random.random() < utils.INTRA_PROBABILITY):
                    random_caller_id = random.choice(user_list)
                    random_receiver_id = random.choice(user_list)
                    cursor.execute(utils.get_mobile_number(random_caller_id))
                    caller_number_list = utils.list_of_tuples_to_list(
                        cursor.fetchone())
                    caller_number = caller_number_list[0]
                    cursor.execute(utils.get_mobile_number(random_receiver_id))
                    receiver_number_list = utils.list_of_tuples_to_list(
                        cursor.fetchone())
                    receiver_number = receiver_number_list[0]
                elif(random.random() < utils.INTER_PROBABILITY_OUTGOING):
                    random_caller_id = random.choice(user_list)
                    random_receiver_id = "0"*32
                    cursor.execute(utils.get_mobile_number(random_caller_id))
                    caller_number_list = utils.list_of_tuples_to_list(
                        cursor.fetchone())
                    caller_number = caller_number_list[0]
                    receiver_number = utils.random_number_generator()
                else:
                    random_caller_id = "0"*32
                    random_receiver_id = random.choice(user_list)
                    caller_number = utils.random_number_generator()
                    cursor.execute(utils.get_mobile_number(random_receiver_id))
                    receiver_number_list = utils.list_of_tuples_to_list(
                        cursor.fetchone())
                    receiver_number = receiver_number_list[0]
                if(random_receiver_id == random_caller_id):
                    continue
                random_caller_id = "8a8093b47bbb3c40017bbe76db410004"
                user_balance_sql = utils.select_user_from_user_balance(
                    random_caller_id)
                cursor.execute(user_balance_sql)
                user_balance = cursor.fetchone()
                if(user_balance == None):
                    user_balance = (0, 0, 0, 0, 0)

                transaction_id = utils.random_id_generator(32)
                get_plan_statement = utils.get_active_plan(
                    random_caller_id, utils.CALL_TYPE_ID)
                cursor.execute(get_plan_statement)
                plan_id = cursor.fetchone()[0]
                cursor.execute(utils.get_use_database_consumption())
                generated_consumption = random.randint(
                    0, user_balance[utils.CALL_TYPE_ID+1])
                amount_charged = 0
                timestamp = utils.get_current_time()
                data = {
                    "id": random_caller_id,
                    "call": generated_consumption,
                    "data": 0,
                    "sms": 0,
                    "ott": 0
                }
                # await insert_execute(transaction_id, plan_id, random_caller_id, random_receiver_id, caller_number, receiver_number, generated_consumption, amount_charged, timestamp)
                # requests.post(utils.POST_REQUEST_URL, data=data)
                await sync_to_async(debug_logger)("1 Call Transaction inserted")
            # cursor.execute(utils.pseudodrop())
        return True
    except Error as e:  # pragma: no cover
        await sync_to_async(error_logger)(e)
        return False


def call_main():  # pragma: no cover
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    var = loop.run_until_complete(call_transaction())
    return var


if __name__ == '__main__':  # pragma: no cover
    call_main()
