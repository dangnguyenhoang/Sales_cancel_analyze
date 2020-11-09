import sqlite3
import pandas as pd
import numpy as np
import gtts
import matplotlib.pyplot as plt
import shutil
import mysql.connector as myconnect
import csv
import json
import pathlib
import os
import sys
import re
import socket
import datetime
import xlsxwriter
from xlsxwriter.workbook import Workbook
class extractData:
    def __init__(self, raw, date):
        self.raw=raw
        # self.db_name=db_name
        self.date=date.strftime('%Y''-''%m''-''%d''-''%H''%M''%S')
    def print_machine_info(self):
        host_name = socket.gethostname()
        ip_address = socket.gethostbyname(host_name)
        print("Host name: %s" % host_name)
        print("IP address: %s" % ip_address)
    def extract_csv(self):
        database_path=pathlib.Path(self.raw).parent
        shutil.unpack_archive(self.raw,database_path.parent,'zip')
        csv_list=[item for item in database_path.parent.glob('**/*') if item.name.endswith('csv')]
        database=os.path.join(database_path.parent,f"{self.date}.db")
        sum_data={}
        for item in csv_list:
            conn=sqlite3.connect(database)
            cursor= conn.cursor()
            df=pd.read_csv(item)
            col=[item.replace(' ','_') for item in list(df.columns)]
            table=f'CREATE TABLE IF NOT EXISTS {item.name[:-4]} {tuple(col)}'
            print(f'Table Created. Table name: {item.name[:-4]}')
            cursor.execute(table)
            sum_data[item.name[:-4]]=col
            with open (item,'r') as file_1:
                conn=sqlite3.connect(database)
                cursor= conn.cursor()
                numberOfValues=','.join(['?' for number in range(len(col))])
                reader= csv.reader(file_1)
                db_sales = np.array(pd.DataFrame(reader))

                sql_sales=f"INSERT INTO {item.name[:-4]}  {tuple(col)}  VALUES ({numberOfValues})"
                cursor.executemany(sql_sales, db_sales[1::])
                count=f'SELECT Count(*) FROM {item.name[:-4]}'
                count=(cursor.execute(count)).fetchone()
                conn.commit()
                print(f'File: {item.name} extracted, {count[0]} rows, {len(col)} columns')
                conn.close()
        return database

    def combine_data(self):
        database_path=pathlib.Path(self.raw).parent
        path=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(path)
        cursor= conn.cursor()
        table="CREATE TABLE IF NOT EXISTS Combined_table (ID INTEGER PRIMARY KEY, Item_Id, Order_No, Order_Date, Address_Id, Order_Qty, Shipped DEFAULT 0, Canceled DEFAULT 0, Status)"
        print('Combined table created')
        cursor.execute(table)

        copy_sales = "INSERT INTO Combined_table (Item_Id, Order_No, Order_Date, Address_Id, Order_Qty, Shipped) SELECT Item_Code, Order_No, Order_Date, Ship_To_Address_No, Order_Qty, Quantity_Shipped FROM sales_test;"
        cursor.execute(copy_sales)
        copy_canceled = "INSERT INTO Combined_table (Item_Id, Order_No, Order_Date, Address_Id, Order_Qty, Canceled) SELECT Item_Code, Order_Number, Order_Date, Ship_To_Address_No, Quantity_Ordered, Quantity_Canceled FROM canceled_test;"
        cursor.execute(copy_canceled)
       
        update_shipped = """UPDATE Combined_table
        SET Status = 'All Shipped'
            WHERE Order_Qty>0 AND Order_Qty = Shipped
        """
        cursor.execute(update_shipped)

        update_canceled = """UPDATE Combined_table
        SET Status = 'All Canceled'
            WHERE Order_Qty>0 AND Order_Qty = Canceled
        """
        cursor.execute(update_canceled)
        conn.commit()
        conn.close()

    def sum_table(self):
        database_path=pathlib.Path(self.raw).parent
        database=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(database)
        cursor=conn.cursor()

        sum_total_table= "CREATE TABLE IF NOT EXISTS Summary_by_Product (Id int AUTO_INCREMENT, Items_Shipped DEFAULT 0, Items_Canceled DEFAULT 0, Items_Shipped_ratio DEFAULT 0, Items_Canceled_ratio DEFAULT 0, Report_time DEFAULT 0)"
        sum_Addr_table = "CREATE TABLE IF NOT EXISTS Summary_by_Address (Id, Addr ,Shipped DEFAULT 0, Canceled DEFAULT 0,  Addr_Shipped_ratio DEFAULT 0, Addr_Canceled_ratio DEFAULT 0, Report_time DEFAULT 0 )"
        cursor.execute(sum_total_table)
        cursor.execute(sum_Addr_table)
        sum_total_content= f"""INSERT INTO Summary_by_Product 
            (Items_Shipped, Items_Canceled,Items_Shipped_ratio, Items_Canceled_ratio, Report_time) 
            VALUES ((SELECT SUM(Shipped) FROM Combined_table WHERE Shipped>0),
                    (SELECT SUM(Canceled) FROM Combined_table WHERE Canceled>0),
                    (SELECT SUM(Shipped) FROM Combined_table WHERE Shipped>0)/((SELECT SUM(Order_Qty) FROM Combined_table WHERE Order_Qty>0)),
                    (SELECT SUM(Canceled) FROM Combined_table WHERE Canceled>0)/((SELECT SUM(Order_Qty) FROM Combined_table WHERE Order_Qty>0)),
                    (SELECT datetime((SELECT strftime('%s','now')), 'unixepoch','localtime')))
            """
        cursor.execute(sum_total_content)
        sum_Addr_content= f"""INSERT INTO Summary_by_Address 
        (Addr) 
        VALUES ((SELECT DISTINCT (Address_Id)  FROM Combined_table))"""
        cursor.execute(sum_Addr_content)
        conn.commit()
        conn.close()
        
    def to_excel(self):
        workbook=Workbook(f'{self.date}.xlsx')
        database_path=pathlib.Path(self.raw).parent
        database=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(database)
        cursor=conn.cursor()
        cursor.execute(f"""SELECT name FROM sqlite_master WHERE type ='table' AND  name NOT LIKE 'sqlite_%'""")
        table_name=cursor.fetchall()
        table_name=[item[0] for item in table_name]
        for item in table_name:

            sheet=workbook.add_worksheet(name=item,)
            cursor.execute(f"""SELECT * FROM {item};""")
            sheet_data=cursor.execute(f"SELECT * FROM {item}")
            column=[name[0] for name in list(sheet_data.description)]
            for i, row in enumerate(sheet_data):
                for j, value in enumerate(row):
                    sheet.write(i+1,j,value)
            for m in range(len(column)):
                sheet.write(0,m,column[m])
        workbook.close()
       

