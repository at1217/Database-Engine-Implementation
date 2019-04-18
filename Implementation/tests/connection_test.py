from Implementation.src import CSVDataTable
import logging

logging.basicConfig(level=logging.DEBUG)

def t1():
    t = CSVDataTable.CSVDataTable(table_name="Test", column_names=['foo','bar'], primary_key_columns=['foo'], loadit=None)
    print("t",t)

t1()