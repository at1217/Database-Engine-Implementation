from Implementation.src import CSVDataTable
import logging
import csv

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
def delete():
    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)

    t.import_data(new_r)
    #t.add_index('last_name','first_name', "INDEX")

    temp = {'uni':'og11','last_name':'Gemgee','first_name':'Gaffer'}

    #result = t.find_by_template(temp,fields=None, use_index=True)

    result = t.delete(temp)
    print(t)






delete()