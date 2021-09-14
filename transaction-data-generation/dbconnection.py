import mysql.connector
from pymongo import MongoClient


class SqlManager:
    # initialize parameters for connection establishment
    def __init__(self, host, user, password, ignore_error=False):
        self.host = host
        self.user = user
        self.password = password
        self.ignore_error = ignore_error

    # establishes connection to database
    def __enter__(self):
        self.connection = mysql.connector.connect(user=self.user, password=self.password,
                                                  host=self.host)
        return self.connection

    # close database connection
    def __exit__(self, ex_type, ex_value, traceback):
        self.connection.close()
        if ex_type is not None:  # pragma: no cover
            return self.ignore_error
