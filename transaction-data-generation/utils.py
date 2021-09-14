import random
import string
import numpy as np
import time
import datetime
import os
import platform
import sql_queries

LIST_OF_CONSUMPTION = ["data", "ott", "calls_incoming",
                       "calls_outgoing", "messages_incoming", "messages_outgoing"]

LIST_OF_SERVICES = ["data", "ott", "calls", "messages"]

DATA_TYPE_ID = 0
CALL_TYPE_ID = 2
MESSAGE_TYPE_ID = 3
POST_REQUEST_URL = "https://httpbin.org/post"

BASE_PATH = ""

if platform.system() == "Windows":
    BASE_PATH = os.path.dirname(os.path.realpath(__file__))+"\\"
else:  # pragma: no cover
    BASE_PATH = os.path.dirname(os.path.realpath(__file__))+"/"

NO_OF_USERS = 50
NO_OF_TRANSACTION = 10
NO_OF_CONSUMPTION_TYPES = len(LIST_OF_CONSUMPTION)
NO_OF_SERVICES = len(LIST_OF_SERVICES)
MAX_MESSEAGE_BALANCE = 30
MAX_CALL_BALANCE = 86400  # in seconds
MAX_DATA_BALANCE = 2**20  # in bytes
MAX_OTT_BALANCE = 24*60  # in minutes
SAMPLE_SIZE = 20
TRANSACTION_INTERVAL = 2  # in seconds
NUMBER_OF_USERS_PICKED = 5000
INTRA_PROBABILITY = 0.33
INTER_PROBABILITY_OUTGOING = 0.5
NUMBER_OF_PLANS_PICKED = 10

# Generate a 24 char long random id


def random_id_generator(length):
    res = ''.join(random.choices(string.ascii_lowercase +
                                 string.digits, k=length))
    return (str(res))

# Generate a 10 digit random mobile no with country code


def random_number_generator():  # pragma: no cover
    n = '0000000000'
    while '9' in n[3:6] or n[3:6] == '000' or n[6] == n[7] == n[8] == n[9]:
        n = str(random.randint(10**9, 10**10-1))
    return n[:3] + n[3:6] + n[6:]

# returns time stamp in mysql format


def get_current_time():
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(
        ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

# random distrution generator using poission


def random_distrubution_generator(size, scale):
    s = np.random.poisson(10, size)
    l = np.int64(s)
    if(scale < 2**31):
        for i in range(size):
            s[i] = s[i] if s[i] < 20 else 19
            s[i] = s[i] * scale + random.randint(0, scale)
        return s
    else:
        for i in range(size):
            l[i] = l[i] if l[i] < 20 else 19
            l[i] = l[i] * scale + int(float(random.getrandbits(128) % (scale)))
        return l

# returns a user balance in form of list


def user_balance_generator(size):
    balance_user = [random_id_generator(32) for _ in range(size)]
    balance_message = random_distrubution_generator(
        size, MAX_MESSEAGE_BALANCE//SAMPLE_SIZE)
    balance_calls = random_distrubution_generator(
        size, MAX_CALL_BALANCE//SAMPLE_SIZE)
    balance_data = random_distrubution_generator(
        size, MAX_DATA_BALANCE//SAMPLE_SIZE)
    balance_ott = random_distrubution_generator(
        size, MAX_OTT_BALANCE//SAMPLE_SIZE)
    return [balance_user, balance_data, balance_ott, balance_calls, balance_message]


def select_user_from_user_balance(user_id):
    sql = """
        select * from user_balance where id = '{}'
    """.format(user_id)
    return sql


def pick_users_from_user_balance(offset, number_of_users):
    sql = """
        select id from user_balance limit {},{}
    """.format(offset, number_of_users)
    return sql


def pick_users_from_user_detail():
    sql = """
        select id from user_detail;
    """
    return sql


def list_of_tuples_to_list(list_of_tuples):
    import itertools
    return list(itertools.chain(*list_of_tuples))


def select_id_from_transaction_type():
    sql = """
        select id from transaction_type
    """
    return sql


def size_of_user_balance_table():
    sql = """
        select count(*) from user_balance
    """
    return sql


def size_of_user_detail_table():
    sql = """
        select count(*) from user_detail
    """
    return sql


def get_mobile_number(user_id):
    sql = """
        select mobile from authdb.user_detail where id = "{}";
    """.format(user_id)
    return sql


def sub(entity1, entity2):
    return entity1 - entity2


def get_log_file():
    return 'transaction1.log'


def get_create_database_consumption():
    return sql_queries.createDatabaseConsumption


def get_use_database_consumption():
    return sql_queries.useDatabaseConsumption


def get_use_database_authdb():
    return sql_queries.useDatabaseAuthdb


def get_create_table_user_balance():
    return sql_queries.createTableUserBalance


def get_create_table_data():
    return sql_queries.createTableData


def get_create_table_message():
    return sql_queries.createTableMessage


def get_create_table_call():
    return sql_queries.createTableCall


def pseudodrop():
    return ""


def get_active_plan(user_id, service_id):
    sql = """
        select plan_id from transactions.active_plan where user_id = "{}" and service_id = {}
    """.format(user_id, service_id)
    return sql


def get_use_database_transaction():
    return sql_queries.use_database_transaction


data_plan_id_list = ["613d78f3bdd03c3c79706817",
                     "613d78f3bdd03c3c79706818",
                     "613d78f4bdd03c3c79706819",
                     "613d78f4bdd03c3c7970681a",
                     "613d78f5bdd03c3c7970681b",
                     "613d78f5bdd03c3c7970681c",
                     "613d78f6bdd03c3c7970681d",
                     "613d78f6bdd03c3c7970681e",
                     "613d78f7bdd03c3c7970681f",
                     "613d78f7bdd03c3c79706820",
                     "613d78f8bdd03c3c79706821",
                     "613d78f8bdd03c3c79706822",
                     "613d78f9bdd03c3c79706823",
                     "613d78f9bdd03c3c79706824",
                     "613d78fabdd03c3c79706825",
                     "613d78fabdd03c3c79706826",
                     "613d78fbbdd03c3c79706827",
                     "613d78fbbdd03c3c79706828",
                     "613d790fbdd03c3c7970684d",
                     "613d790fbdd03c3c7970684e",
                     "613d7910bdd03c3c7970684f",
                     "613d7911bdd03c3c79706850",
                     "613d7911bdd03c3c79706851",
                     "613d7912bdd03c3c79706852",
                     "613d7912bdd03c3c79706853",
                     "613d7913bdd03c3c79706854",
                     "613d7913bdd03c3c79706855",
                     "613d7914bdd03c3c79706856",
                     "613d7915bdd03c3c79706857",
                     "613d7915bdd03c3c79706858",
                     "613d7916bdd03c3c79706859",
                     "613d7916bdd03c3c7970685a",
                     "613d7917bdd03c3c7970685b",
                     "613d7917bdd03c3c7970685c",
                     "613d7918bdd03c3c7970685d",
                     "613d7918bdd03c3c7970685e"]

call_plan_id_list = ["613d78e0bdd03c3c797067f3",
                     "613d78e0bdd03c3c797067f4",
                     "613d78e1bdd03c3c797067f5",
                     "613d78e2bdd03c3c797067f6",
                     "613d78e2bdd03c3c797067f7",
                     "613d78e3bdd03c3c797067f8",
                     "613d78e3bdd03c3c797067f9",
                     "613d78e4bdd03c3c797067fa",
                     "613d78e4bdd03c3c797067fb",
                     "613d78e5bdd03c3c797067fc",
                     "613d78e5bdd03c3c797067fd",
                     "613d78e6bdd03c3c797067fe",
                     "613d78e6bdd03c3c797067ff",
                     "613d78e7bdd03c3c79706800",
                     "613d78e7bdd03c3c79706801",
                     "613d78e8bdd03c3c79706802",
                     "613d78e8bdd03c3c79706803",
                     "613d78e9bdd03c3c79706804",
                     "613d78fcbdd03c3c79706829",
                     "613d78fdbdd03c3c7970682a",
                     "613d78fdbdd03c3c7970682b",
                     "613d78febdd03c3c7970682c",
                     "613d78febdd03c3c7970682d",
                     "613d78ffbdd03c3c7970682e",
                     "613d78ffbdd03c3c7970682f",
                     "613d7900bdd03c3c79706830",
                     "613d7900bdd03c3c79706831",
                     "613d7901bdd03c3c79706832",
                     "613d7901bdd03c3c79706833",
                     "613d7902bdd03c3c79706834",
                     "613d7902bdd03c3c79706835",
                     "613d7903bdd03c3c79706836",
                     "613d7903bdd03c3c79706837",
                     "613d7904bdd03c3c79706838",
                     "613d7904bdd03c3c79706839",
                     "613d7905bdd03c3c7970683a"]

sms_plan_id_list = ["613d78e9bdd03c3c79706805",
                    "613d78eabdd03c3c79706806",
                    "613d78eabdd03c3c79706807",
                    "613d78ebbdd03c3c79706808",
                    "613d78ebbdd03c3c79706809",
                    "613d78ecbdd03c3c7970680a",
                    "613d78ecbdd03c3c7970680b",
                    "613d78edbdd03c3c7970680c",
                    "613d78edbdd03c3c7970680d",
                    "613d78eebdd03c3c7970680e",
                    "613d78efbdd03c3c7970680f",
                    "613d78efbdd03c3c79706810",
                    "613d78f0bdd03c3c79706811",
                    "613d78f0bdd03c3c79706812",
                    "613d78f1bdd03c3c79706813",
                    "613d78f1bdd03c3c79706814",
                    "613d78f2bdd03c3c79706815",
                    "613d78f2bdd03c3c79706816",
                    "613d7905bdd03c3c7970683b",
                    "613d7906bdd03c3c7970683c",
                    "613d7906bdd03c3c7970683d",
                    "613d7907bdd03c3c7970683e",
                    "613d7907bdd03c3c7970683f",
                    "613d7908bdd03c3c79706840",
                    "613d7908bdd03c3c79706841",
                    "613d7909bdd03c3c79706842",
                    "613d7909bdd03c3c79706843",
                    "613d790abdd03c3c79706844",
                    "613d790abdd03c3c79706845",
                    "613d790bbdd03c3c79706846",
                    "613d790cbdd03c3c79706847",
                    "613d790cbdd03c3c79706848",
                    "613d790dbdd03c3c79706849",
                    "613d790dbdd03c3c7970684a",
                    "613d790ebdd03c3c7970684b",
                    "613d790ebdd03c3c7970684c"]
