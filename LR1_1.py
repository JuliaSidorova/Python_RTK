import json, psycopg2
from psycopg2 import Error
from config import *

if __name__ == '__main__':
    print("Лабораторная работа №1.")
    print("Задание 1.")
    print("получаем данные в data")
    try:
        with open(FILE_NAME, "r", encoding='utf-8') as read_file:
            data = json.load(read_file)
    except Exception as err:
        print(err)

    print("коннект к базе и запись")
    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASS,
            host=HOST,
            port=PORT
        )

        create_table = """ CREATE TABLE IF NOT EXISTS okved(
        code TEXT, parent_code TEXT, section TEXT, name TEXT, comment TEXT)"""
        cur = connection.cursor()
        cur.execute(create_table)
        connection.commit()

        insert_query = """
        insert into okved  (code, parent_code, section, name, comment) 
        values (%(code)s, %(parent_code)s, %(section)s, %(name)s, %(comment)s) 
        """
        cur = connection.cursor()
        cur.executemany(insert_query, data)
        connection.commit()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cur.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")