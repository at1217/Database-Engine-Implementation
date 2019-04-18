#Amon Tokoro
#at3250


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

def test():
    new_r, cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols,
                                  primary_key_columns=['uni'],
                                  loadit=None)
    t.import_data(new_r)

    t.save()


test()