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
            print(f'SQL table Created. Table name: {item.name[:-4]}')
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
                print(f'File: {item.name} extracted to SQL Table {item.name[:-4]} , {count[0]} rows, {len(col)} columns')
                conn.close()
        return database

    def combine_data(self):
        database_path=pathlib.Path(self.raw).parent
        path=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(path)
        cursor= conn.cursor()
        table="CREATE TABLE IF NOT EXISTS Combined_table (ID INTEGER PRIMARY KEY, Item_Id, Order_No, Order_Date, Address_Id, Order_Qty INT, Shipped INT DEFAULT 0, Canceled INT DEFAULT 0, Status)"
   
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
        print(f'SQL database {database} Created')
        cursor=conn.cursor()

        sum_total_table= "CREATE TABLE IF NOT EXISTS Total_Summary (Category TEXT PRIMARY KEY, Total_Shipped DEFAULT 0, Total_Canceled DEFAULT 0, Shipped_ratio DEFAULT 0, Canceled_ratio DEFAULT 0, A_class, B_class, C_class,Report_time DEFAULT 0)"
        sum_Addr_table = "CREATE TABLE IF NOT EXISTS Summary_by_Address ( Addr INT PRIMARY KEY, Total_Order INT, Shipped INT, Canceled INT, Shipped_ratio, Canceled_ratio, ABC_Classification)"
        sum_IC_table = "CREATE TABLE IF NOT EXISTS Summary_by_IC (Item_code INT PRIMARY KEY, Total_Order INT, Shipped INT, Canceled INT,Shipped_ratio, Canceled_ratio, ABC_Classification)"
        cursor.execute(sum_Addr_table)
        cursor.execute(sum_IC_table)
        cursor.execute(sum_total_table)
        
        
        
        sum_Addr_content= f"""INSERT INTO Summary_by_Address 
        (Addr,Total_Order, Shipped, Canceled) 
        SELECT Address_Id, SUM(Order_Qty),SUM(Shipped), SUM(Canceled) FROM Combined_table GROUP BY Address_Id"""
        cursor.execute(sum_Addr_content)
        update_Addr_content=f"""UPDATE  Summary_by_Address
        SET 
            Shipped_ratio= ROUND(100*Shipped/Total_Order,3), 
            Canceled_ratio=ROUND(100*Canceled/Total_Order,3),
            ABC_Classification = 
               (CASE 
                    WHEN 100*Shipped/Total_Order<=80*1 THEN 'A'
                    WHEN 100*Shipped/Total_Order>95*1 THEN 'C'
                    ELSE 'B'
                END)
                WHERE Total_Order>0
        """
        cursor.execute(update_Addr_content)
        
        sum_IC_content = f"""INSERT INTO Summary_by_IC
        (Item_code,Total_Order, Shipped, Canceled) 
        SELECT Item_Id, SUM(Order_Qty),SUM(Shipped), SUM(Canceled) FROM Combined_table GROUP BY Item_Id"""
        cursor.execute(sum_IC_content)
        
        update_IC_content=f"""UPDATE  Summary_by_IC
        SET 
            Shipped_ratio= ROUND(100*Shipped/Total_Order,3), 
            Canceled_ratio=ROUND(100*Canceled/Total_Order,3),
            ABC_Classification = 
               (CASE 
                    WHEN 100*Shipped/Total_Order<=80*1 THEN 'A'
                    WHEN 100*Shipped/Total_Order>95*1 THEN 'C'
                    ELSE 'B'
                END)
                WHERE Total_Order>0
        """ 
        cursor.execute(update_IC_content)
        
        # sum_total_content= f"""INSERT INTO Total_Summary 
        #     (Category, Total_Shipped, Total_Canceled,Shipped_ratio, Canceled_ratio, A_class, B_class, C_Class, Report_time) 
        #     VALUES (SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0),
        #             (SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0),
        #             (SELECT SUM(Canceled) FROM Summary_by_Address WHERE Canceled>0),
        #             (SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0)/(SELECT SUM(Total_Order) FROM Summary_by_Address WHERE Total_Order>0)),
        #             (SELECT SUM(Canceled) FROM Summary_by_Address WHERE Canceled>0)/(SELECT SUM(Total_Order) FROM Summary_by_Address WHERE Total_Order>0)),
        #             (SELECT SUM(Shipped) FROM Summary_by_Address WHERE ABC_Classification=A)/(SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0),
        #            (SELECT SUM(Shipped) FROM Summary_by_Address WHERE ABC_Classification=B)/(SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0), 
        #            (SELECT SUM(Shipped) FROM Summary_by_Address WHERE ABC_Classification=C)/(SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0), 
        #             (SELECT datetime((SELECT strftime('%s','now')), 'unixepoch','localtime')))
    #         """
        sum_total_content= f"""UPDATE Total_Summary 
                            SET
                            Category= "By_Address",
                            Total_Shipped= (SELECT SUM(Shipped) FROM Summary_by_Address WHERE Shipped>0),
                            Total_Canceled= (SELECT SUM(Canceled) FROM Summary_by_Address WHERE Canceled>0),
                            Shipped_ratio=((SELECT SUM(Shipped) FROM Summary_by_Address)/(SELECT SUM(Total_Order) FROM Summary_by_Address)),
                            Report_time=(SELECT datetime((SELECT strftime('%s','now')), 'unix epoch','localtime'))
                   
                            WHERE rowid=1
                    
                    """
        cursor.execute(sum_total_content)
        conn.commit()
        conn.close()
        print('Summary Tables created')
        
    def to_excel(self):
        workbook=Workbook(f'{self.date}.xlsx')
        print(f'{self.date} Workbook Created. Adding worksheets') 
        database_path=pathlib.Path(self.raw).parent
        database=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(database)
        cursor=conn.cursor()
        cursor.execute(f"""SELECT name FROM sqlite_master WHERE type ='table' AND  name NOT LIKE 'sqlite_%'""")
        table_name=cursor.fetchall()
        table_name=[item[0] for item in table_name]
        with pd.ExcelWriter(f'{self.date}.xlsx') as writer: 
            for item in table_name:
                cursor.execute(f"""SELECT * FROM {item};""")
                columns=cursor.execute(f"""SELECT * FROM {item};""") 
                sheet_data=pd.DataFrame(cursor.fetchall())
                sheet_data.columns=[i[0] for i in list(columns.description)]
                sheet_data.to_excel(writer, sheet_name=item)
                print(item,'worksheet added')
            
    def to_csv(self):
        database_path=pathlib.Path(self.raw).parent
        database=os.path.join(database_path.parent,f'{self.date}.db')
        conn=sqlite3.connect(database)
        cursor=conn.cursor()
        cursor.execute("SELECT * FROM Summary_by_Address")
        df=pd.DataFrame(cursor.fetchall())
        df.columns=[i[0] for i in list(cursor.description)]
        df.to_csv('./data/combined_data.csv')
        conn.close()
