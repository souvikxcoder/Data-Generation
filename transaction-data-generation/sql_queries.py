createDatabaseConsumption = """
    CREATE DATABASE IF NOT EXISTS consumption
"""

useDatabaseConsumption = """
    USE consumption
"""

use_database_transaction = """
    USE transactions
"""

useDatabaseAuthdb = """
    USE finaldb
"""

createTableUserBalance = """
    CREATE TABLE IF NOT EXISTS user_balance (
        id varchar(255) primary key,
        data_service bigint,
        ott_service int,
        call_service int,
        sms_service int
    )
"""

createTableService = """
    CREATE TABLE IF NOT EXISTS service (
        id tinyint primary key,
        name varchar(30)
    )
"""

createTableActivePlan = """
    CREATE TABLE IF NOT EXISTS active_plan (
        id int [pk]
        user_id varchar(32)
        service_id tinyint
        plan_id varchar(32)
    )
"""

createTableData = """
    CREATE TABLE IF NOT EXISTS data (
        id varchar(32) primary key,
        plan_id varchar(32),
        user_id varchar(32),
        kb_consumed int,
        amount_charged double,
        created_at timestamp
    )
"""

createTableCall = """
    CREATE TABLE IF NOT EXISTS `call` (
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
    )
"""

createTableMessage = """
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
"""

createTableOTT = """
    CREATE TABLE IF NOT EXISTS ott (
        id varchar(32) primary key,
        user_id varchar(32),
        plan_id varchar(32),
        channel_id varchar(32),
        package_id varchar(32),
        mins_consumed int,
        created_at timestamp
    )
"""

createTablePackage = """
    CREATE TABLE IF NOT EXISTS package (
        id varchar(32) primary key,
        name varchar(32)
    )
"""

createTableChannel = """
    CREATE TABLE IF NOT EXISTS channel (
        id varchar(32) primary key,
        name varchar(32)
    )
"""

createTableChannelPackage = """
    CREATE TABLE IF NOT EXISTS channel_package (
        id varchar(32) primary key,
        channel_id varchar(32),
        package_id varchar(32),
        FOREIGN KEY (channel_id) REFERENCES channel(id),
        FOREIGN KEY (package_id) REFERENCES package(id)
    )
"""
