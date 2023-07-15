import zipfile, json, psycopg2
import pandas as pd
from psycopg2 import Error
from config import *
from sqlalchemy import create_engine

if __name__ == '__main__':
    print("Лабораторная работа №1.")
    print("Задание 2.")

    try:
        connection = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASS,
            host=HOST,
            port=PORT
        )

        create_table = """ CREATE TABLE IF NOT EXISTS telecom_companies(
        ogrn TEXT, inn TEXT, kpp  TEXT, name TEXT, okved TEXT)"""
        cur = connection.cursor()
        cur.execute(create_table)
        connection.commit()

        alchemyEngine = create_engine(STR_CONNECTION)
        postgreSQLConnection = alchemyEngine.connect()

        f = 0
        try:
            with zipfile.ZipFile(FILE_NAME_ZIP, 'r') as zipobj:
                file_names = zipobj.namelist()
                for name in file_names:
                    print(f,' - ',name)
                    with zipobj.open(name, 'r') as file:
                        json_data = file.read()
                        data = json.loads(json_data)

                        df = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')

                        if {"ogrn", "inn", "kpp", "name", "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД"}.issubset(df.columns):
                            df = df[["ogrn", "inn", "kpp", "name", "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД"]]
                            df.columns = ["ogrn", "inn", "kpp", "name", "okved"]

                            df = df[df['okved'].str.startswith('61', na=False)]

                            df.to_sql(TABLE_LAB1, postgreSQLConnection, if_exists='append', index=False)
                            postgreSQLConnection.commit()
                        else:
                            print(f"{name} - Нет нужного столбца")
                        f = f + 1
        except Exception as error:
            print(error)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cur.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")
