import matplotlib.pyplot as plt
import shutil
import mysql.connector as myconnect
import csv
import database
import json
import pathlib
import database
import os
import sqlite3
import numpy as np
import pandas as pd
import re
# import extractdata
import datetime
raw=database.raw_file


if __name__ == '__main__':
    raw=database.raw_file
    # db_name=str(input('Please set the database name:\n'))
    # data=extractdata.extractData(raw,db_name)
    # data.print_machine_info()
    # data.extract_csv()
    # data.combine_data()
    # data.sum_table()
    date=datetime.datetime.now()
    # print(f'{date.year}{date.month}{date.date}')
    database_path='./data/2020-11-10-125245.db'
    # database=os.path.join(database_path.parent,f'{date.strftime('%Y''-''%m''-''%d''-''%H''%M''%S')}.db')
    conn=sqlite3.connect(database_path)
    cursor=conn.cursor()
    # cursor.execute(f"""SELECT * FROM Summary_by_Address""")
    sheet_data=cursor.execute(f"SELECT * FROM Summary_by_Address")
    l=[row for row in sheet_data]
    df=pd.DataFrame(l)
    df.columns=[i[0] for i in list(sheet_data.description)]
    print(df.head(5))
    cursor.execute('SELECT * FROM Summary_by_Address')
    a=pd.DataFrame(cursor.fetchall())
    print(a.head())
    # print('row:', row)
    # column=[name[0] for name in list(sheet_data.description)]
    # for i, row in enumerate(sheet_data):
    #     for j, value in enumerate(row):
    #         sheet.write(i+1,j,value)