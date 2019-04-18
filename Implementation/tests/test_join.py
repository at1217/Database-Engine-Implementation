from Implementation.src import CSVDataTable
import logging
import csv
import json

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
def join():
    t_rows, t_cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=t_cols, primary_key_columns=['playerID'])
    t.import_data(t_rows)
    print("T = ", t)


    t2_rows, t2_cols = load("/Users/amon/Documents/GitHub/W4111-HW3/Data/Batting.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="Batting", column_names=t2_cols, primary_key_columns=['playerID', 'teamID','yearID','stint'])
    t2.import_data(t2_rows)
    print("T2 = ", t2)

    j = t2.join(t, ['playerID'],
                where_clause={"People.nameLast": "Williams", "People.birthCity": "San Diego"},
                fields= ["playerID", "People.nameLast", "People.nameFirst","Batting.teamID",
                         "Batting.yearID", "Batting.stint", "Batting.H", "Batting.AB"], optimize=True)

    print("Result = ", j)
    print("All rows = ", json.dumps(j._rows, indent=2))


join()