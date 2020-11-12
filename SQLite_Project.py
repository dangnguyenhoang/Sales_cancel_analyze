
import database
import extractdata
import datetime

if __name__ == '__main__':
    raw=database.raw_file
    current_time=datetime.datetime.now()
    data=extractdata.extractData(raw,current_time)
    data.print_machine_info()
    data.extract_csv()
    data.combine_data()
    data.sum_table()
    data.to_excel()
    data.to_csv()


