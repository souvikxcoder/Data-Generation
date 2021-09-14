from mysql.connector import Error
import mysql.connector
import sql_queries
import utils
import credentials
import dbconnection

# setting up connection


def user_balance():
    try:
        with dbconnection.SqlManager(credentials.connection_details_local["host"],
                                     credentials.connection_details_local["user"],
                                     credentials.connection_details_local["key"]) as connect:
            global connection
            global cursor
            connection = connect
            cursor = connection.cursor()

            cursor.execute(utils.get_use_database_authdb())
            num_records_user_balance_stmt = utils.size_of_user_detail_table()
            cursor.execute(num_records_user_balance_stmt)
            num_records_user_balance = cursor.fetchone()
            if(num_records_user_balance):
                num_records_user_balance = num_records_user_balance[0]
            cursor.execute(utils.pick_users_from_user_detail())
            list_of_tuples = cursor.fetchall()
            user_list = utils.list_of_tuples_to_list(list_of_tuples)

            # sql statements to create database and tables
            cursor.execute(utils.get_create_database_consumption())
            cursor.execute(utils.get_use_database_consumption())
            cursor.execute(utils.get_create_table_user_balance())

            # generating user balance list using poission distribution
            balance = utils.user_balance_generator(num_records_user_balance)
            for i in range(num_records_user_balance):
                # inserting data in  table
                insert_statement = """
                INSERT INTO user_balance(id, data, ott, calls, messages) values (
                        '{}',
                        {},
                        {},
                        {},
                        {}
                    )
                """.format(user_list[i], balance[1][i], balance[2][i], balance[3][i], balance[4][i])
                try:
                    cursor.execute(insert_statement)
                    connection.commit()
                except Error as e:  # pragma: no cover
                    print(e)
            cursor.execute(utils.pseudodrop())
        return True
    except Error as e:  # pragma: no cover
        print(e)


if __name__ == '__main__':  # pragma: no cover
    user_balance()
