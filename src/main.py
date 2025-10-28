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
            try:
                create_table_query = "CREATE TABLE IF NOT EXISTS 'rooms' ;"
                cursor.execute(create_table_query)
                print('Table created successfully')
            except Exception as e:
                print(e)



    finally:
        mydb.close()
except Exception as ex:
    print(
        'Connection unsuccessful. '
    )
    print(ex)
