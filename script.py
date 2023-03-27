from datetime import datetime, timedelta
import time
from typing import List

import sqlite3

import openpyxl as xls

START_DATE = datetime(2023, 1, 2)
DATE_FORMAT = "%Y-%m-%d"
FILENAME = "file1.xlsx"
LISTNAME = 'Лист1'


class DataBase:
    @staticmethod
    def sqlite_connection(func):
        def wrapper(*args, **kwargs):
            with sqlite3.connect('db.db') as con:
                kwargs['con'] = con
                res = func(*args, **kwargs)
                con.commit()
            return res

        return wrapper

    @staticmethod
    @sqlite_connection.__func__
    def init_db(con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS COMPANIES (
                    COMPANY_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    COMPANY_NAME TEXT
                );""")
        cur.execute("""
                CREATE TABLE IF NOT EXISTS FACTS (
                    FACT_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    COMPANY_ID INTEGER,
                    QLIQ_DATA1 INTEGER,
                    QLIQ_DATA2 INTEGER,
                    QOIL_DATA1 INTEGER,
                    QOIL_DATA2 INTEGER,
                    FACT_DATE INT,
                    FOREIGN KEY (COMPANY_ID) REFERENCES COMPANIES(COMPANY_ID)
                );""")
        cur.execute("""
                CREATE TABLE IF NOT EXISTS FORECASTS (
                    FORECAST_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    COMPANY_ID INTEGER,
                    QLIQ_DATA1 INTEGER,
                    QLIQ_DATA2 INTEGER,
                    QOIL_DATA1 INTEGER,
                    QOIL_DATA2 INTEGER,
                    FORECAST_DATE INT,
                    FOREIGN KEY (COMPANY_ID) REFERENCES COMPANIES(COMPANY_ID)
                );""")


class CompaniesRepository:
    @staticmethod
    @DataBase.sqlite_connection
    def add_company(con: sqlite3.Connection, name: str):
        cur = con.cursor()
        cur.execute("""INSERT INTO COMPANIES (COMPANY_NAME) VALUES (?)""", (name,))

    @staticmethod
    @DataBase.sqlite_connection
    def get_companies(con: sqlite3.Connection) -> List:
        cur = con.cursor()
        cur.execute("""SELECT * FROM COMPANIES;""")
        return cur.fetchall()

    @staticmethod
    @DataBase.sqlite_connection
    def get_company(con: sqlite3.Connection, id: int) -> List:
        cur = con.cursor()
        cur.execute("""SELECT * FROM COMPANIES WHERE COMPANY_ID=(?);""", (id,))
        return cur.fetchone()

    @staticmethod
    @DataBase.sqlite_connection
    def get_company_by_name(con: sqlite3.Connection, name: str) -> List:
        cur = con.cursor()
        cur.execute("""SELECT * FROM COMPANIES WHERE COMPANY_NAME=(?);""", (name,))
        return cur.fetchone()


class FactsRepository:
    @staticmethod
    @DataBase.sqlite_connection
    def add_fact(con: sqlite3.Connection, company_id: int, qliq_data1: int, qliq_data2: int, qoil_data1: int,
                 qoil_data2: int, fact_date: datetime):
        cur = con.cursor()
        cur.execute("""INSERT INTO FACTS (COMPANY_ID, QLIQ_DATA1, QLIQ_DATA2, QOIL_DATA1, QOIL_DATA2, FACT_DATE)
         VALUES (?, ?, ?, ?, ?, ?)""",
                    (company_id, qliq_data1, qliq_data2, qoil_data1, qoil_data2, time.mktime(fact_date.timetuple())))

    @staticmethod
    @DataBase.sqlite_connection
    def get_facts(con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute("SELECT * FROM FACTS;")
        return cur.fetchall()

    @staticmethod
    @DataBase.sqlite_connection
    def get_facts_between_dates(con: sqlite3.Connection, start_date: datetime, end_date: datetime):
        cur = con.cursor()
        cur.execute("SELECT * FROM FACTS WHERE FACT_DATE between (?) and (?);",
                    (time.mktime(start_date.timetuple()), time.mktime(end_date.timetuple())))
        facts = list(map(list, cur.fetchall()))
        for fact in facts:
            fact[-1] = datetime.utcfromtimestamp(fact[-1]).strftime(DATE_FORMAT)
        return facts


class ForecastsRepository:
    @staticmethod
    @DataBase.sqlite_connection
    def add_forecasts(con: sqlite3.Connection, company_id: int, qliq_data1: int, qliq_data2: int, qoil_data1: int,
                      qoil_data2: int, forecast_date: datetime):
        cur = con.cursor()
        cur.execute("""INSERT INTO FORECASTS (COMPANY_ID, QLIQ_DATA1, QLIQ_DATA2, QOIL_DATA1, QOIL_DATA2, FORECAST_DATE)
         VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        company_id, qliq_data1, qliq_data2, qoil_data1, qoil_data2,
                        time.mktime(forecast_date.timetuple())))

    @staticmethod
    @DataBase.sqlite_connection
    def get_forecasts(con: sqlite3.Connection):
        cur = con.cursor()
        cur.execute("""SELECT * FROM FORECASTS;""")
        forecasts = list(map(list, cur.fetchall()))
        for forecast in forecasts:
            forecast[-1] = datetime.utcfromtimestamp(forecast[-1]).strftime(DATE_FORMAT)
        return forecasts

    @staticmethod
    @DataBase.sqlite_connection
    def get_forecasts_between_dates(con: sqlite3.Connection, start_date: datetime, end_date: datetime):
        cur = con.cursor()
        cur.execute("SELECT * FROM FORECASTS WHERE FORECAST_DATE between (?) and (?);",
                    (time.mktime(start_date.timetuple()), time.mktime(end_date.timetuple())))
        return cur.fetchall()


def add_data_to_db():
    DataBase.init_db()
    wb = xls.load_workbook(FILENAME)
    ws = wb[LISTNAME]
    for day, row in enumerate(ws.iter_rows(min_row=4)):
        if CompaniesRepository.get_company_by_name(name=row[1].value) is None:
            CompaniesRepository.add_company(name=row[1].value)
        company = CompaniesRepository.get_company_by_name(name=row[1].value)
        FactsRepository.add_fact(company_id=company[0], qliq_data1=row[2].value, qliq_data2=row[3].value,
                                 qoil_data1=row[4].value, qoil_data2=row[5].value,
                                 fact_date=START_DATE + timedelta(days=day))
        ForecastsRepository.add_forecasts(company_id=company[0], qliq_data1=row[2].value, qliq_data2=row[3].value,
                                          qoil_data1=row[4].value, qoil_data2=row[5].value,
                                          forecast_date=START_DATE + timedelta(days=day))


def main():
    add_data_to_db()
    start_date = datetime(2023, 1, 4)
    end_date = datetime(2023, 1, 7)
    forecasts = ForecastsRepository.get_forecasts_between_dates(start_date=start_date, end_date=end_date)
    print(f'forecast qlib total = {sum([forecast[3] - forecast[2] for forecast in forecasts])}, '
          f'qoil total = {sum([forecast[5] - forecast[4] for forecast in forecasts])}')
    facts = FactsRepository.get_facts_between_dates(start_date=start_date, end_date=end_date)
    print(f'fact qlib total = {sum([fact[3] - fact[2] for fact in facts])}, '
          f'qoil total = {sum([fact[5] - fact[4] for fact in facts])}')


if __name__ == '__main__':
    main()
