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
import extractdata
import datetime
raw=database.raw_file


if __name__ == '__main__':
    # raw=database.raw_file
    # db_name=str(input('Please set the database name:\n'))
    # data=extractdata.extractData(raw,db_name)
    # data.print_machine_info()
    # data.extract_csv()
    # data.combine_data()
    # data.sum_table()
    date=datetime.datetime.now()
    # print(f'{date.year}{date.month}{date.date}')
    print(date.strftime('%Y''%m''%d'))