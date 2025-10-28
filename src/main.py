import json

import pymysql
from config import host, user, password, db_name

with open('rooms.json', 'r', encoding='utf-8') as rooms_file:
    rooms = json.load(rooms_file)

with open('students.json', 'r', encoding='utf-8') as students_file:
    students = json.load(students_file)

try:
    mydb = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print('Connection successful')

    try:
        with mydb.cursor() as cursor:
            for room in rooms:
                pass
            try:
                create_table_query = ("CREATE TABLE rooms (id INT NOT NULL," \
                                      "name VARCHAR(50));")

                cursor.execute(create_table_query)

                print('Table for rooms created successfully')

            except Exception as e:
                print(e)
            try:
                create_students_table_query = ("CREATE TABLE students (birthday DATE NOT NULL,"
                                               "id INT NOT NULL,"
                                               "name VARCHAR(50),"
                                               "room INT,"
                                               "sex VARCHAR(20));")
                cursor.execute(create_students_table_query)
                print('Table for students created successfully')
            except Exception as e:
                print(e)

    finally:
        mydb.close()
        print('Connection closed')
except Exception as ex:
    print(
        'Connection unsuccessful. '
    )
    print(ex)
