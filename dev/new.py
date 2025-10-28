import pymysql
from config import host, user, password, db_name

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Connected!")

    try:
        with connection.cursor() as cursor:
            create_table_query = "CREATE TABLE 'users'(id int AUTO_INCREMENT," \
                                 " name varchar(32),"\
                                 "password varchar(32),"\
                                 "email varchar (32), PRIMARY KEY (id));"
            cursor.execute(create_table_query)
            print("Table successful created!")
    finally:
        connection.close()
except Exception as ex:
    print("connections refused...")
    print(ex)

import pymysql
from config import host, user, password, db_name

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Connected!")

    try:
        with connection.cursor() as cursor:
            create_table_query = "CREATE TABLE users (id INT AUTO_INCREMENT, " \
                                 "name VARCHAR(32), " \
                                 "password VARCHAR(32), " \
                                 "email VARCHAR(32), PRIMARY KEY (id));"
            cursor.execute(create_table_query)
            print("Table successfully created!")
    finally:
        connection.close()
except Exception as ex:
    print("Connection refused...")
    print(ex)

