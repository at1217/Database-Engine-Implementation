from Implementation.src import CSVDataTable
import logging
import csv
import json
import time

logging.basicConfig(level=logging.DEBUG)

def load(fn):
    result = []
    cols = None
    with open(fn,"r") as infile:
        rdr = csv.DictReader(infile)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)

    return result, cols

def test1():

    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)
    t.import_data(new_r)
    print("t = ",t)


def test2():
    i = CSVDataTable.Index(index_name="Smart", index_columns=["last_name", "first_name"], kind="INDEX")
    r = {"last_name": "Jordan", "first_name": "Bob", "uni": "bj1111"}
    kv = i.compute_key(r)
    print("key_value", kv)
    i.add_to_index(row=r, rid="2")
    print("I = ", i)

def test3():
    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)

    r = {"last_name": "Jordan", "first_name": "Bob", "uni": "bj1111", "email": "boo"}

    t.insert(r)
    t.insert(r)
    print("t = ",t)
    cols = ["last_name","first_name"]
    t.add_index("Name", cols, "INDEX")

def test4():

    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols,
                                  primary_key_columns=['playerID'],
                                  loadit=None)
    t.import_data(new_r)
    temp = {'playerID':'willite01'}

    # start = time.time()
    # for i in range(0, 1000):
    #
    #     result = t.find_by_template(temp, fields= ['nameLast','nameFirst'], use_index=False)
    #     if i == 0:
    #         print("result", result)
    #         x = json.dumps(result._rows, indent= 2)
    #         print(x)
    # end = time.time()
    # elapsed = end - start
    # print("Elapsed time = ", elapsed)



    start = time.time()
    for i in range(0, 1000):

        result = t.find_by_template(temp, fields=['nameLast', 'nameFirst'], use_index=True)
        if i == 0:
            print("result", result)
            x = json.dumps(result._rows, indent=2)
            print(x)
    end = time.time()
    elapsed = end - start
    print("Elapsed time = ", elapsed)

def test5():
    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)

    t.import_data(new_r)

    temp = {'uni':'og11','last_name':'Gemgee','first_name':'Gaffer'}

    result = t.delete(temp, new_r)
    print("The number of deleted rows = ",result)

#test1()
#test2()
#test3()
test4()
#test5()