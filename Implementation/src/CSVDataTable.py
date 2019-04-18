import json
import csv
import copy
import logging




class Index():

    def __init__(self, index_name, index_columns, kind):
        self._index_name = index_name
        self._index_columns = index_columns
        self._kind = kind


        self._index_data = None

    def compute_key(self,row):
        key_v = [row[k] for k in self._index_columns]
        key_v = "_".join(key_v)
        return key_v

    def add_to_index(self, row, rid):
        if self._index_data is None:
            self._index_data = {}

        key = self.compute_key(row)
        bucket = self._index_data.get(key, [])
        if self._kind != "INDEX":
            if len(bucket) > 0:
                raise KeyError("Duplicate key")

        bucket.append(rid)
        self._index_data[key] = bucket

    # def __str__(self):
    #     result = ""
    #     result += "Index name: " + self.name
    #     result += "\nKind: " + self._kind
    #     result += "\nIndex columns: " + str(self.index_columns)
    #     result += "\nTable name: " + self.table_name
    #
    #     if self._index_data is not None:
    #         keys = list(self._index_data.keys())
    #         result += "\nNo. of unique index values: " + str(len(keys))
    #         cnt = min(5,len(keys))
    #         result += "Entries:\n"
    #         for i in range(0, cnt):
    #             result += "[" + keys[i] + ":" + json.dumps(self._index_data[keys[i]], indent=2) + "]\n"
    #
    #     return result

    #def remove_from_index(self, row, rid):

    def to_json(self):

        result = {}
        result['name'] = self._index_name
        result['columns'] = self._index_columns
        result['kind'] = self._kind
        #result['table_name'] = self._table_name
        result['index_data'] = self._index_data

        return result

    #def from_json(self,table, loadit):


    def find_rows(self, tmp):

        t_keys = tmp.keys()
        t_vals = [tmp[k] for k in self._index_columns]
        t_s = "_".join(t_vals)
        #self._index_data = get_data(rows, self._index_columns)
        d = self._index_data.get(t_s, None)
        if d is not None:
            d = list(d)

        return d

    def matches_index(self, template):

        k = set(list(template.keys()))
        c = set(self._index_columns)

        if c.issubset(k):
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None

        return kk

    def remove(self, tmp):
        result = self.find_by_template(tmp, fields=None, use_index=True)
        rows = result._rows
        row_id = result.keys()

    def delete_from_index(self, row, rid):
        index_key = self.compute_key(row)

        bucket = self._index_data.get(index_key, None)

        if bucket is not None:
            bucket.remove(rid)

            if len(bucket) == 0:
                del (self._index_data[index_key])


    def get_no_of_entries(self):
        return len(list(self._index_data.keys()))

class CSVDataTable():

    _default_directory = "/Users/amon/Documents/GitHub/W4111-HW3/DB/"

    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):

        self._table_name = table_name
        self._column_names = column_names
        self._primary_key_columns = primary_key_columns

        self._indexes = None

        if not loadit:

            if column_names is None or table_name is None:
                raise ValueError("Please provide table_name for column_names for table create.")

            self._next_row_id = 1

            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")
            # elif primary_key_columns is None and column_names:
            #     for name in column_names


    def get_table_name(self):
        return self._table_name

    def add_index(self, index_name, columns, kind):
        if self._indexes is None:
            self._indexes = {}

        #Check to make sure this is not duplicate index name

        self._indexes[index_name] = Index(index_name=index_name, index_columns=columns, kind=kind)
        self.build(index_name)

    #def drop_index(self, index_name):

    #def __str__(self):

    def get_primary_key(self,r): # Assume r is an OrderDict (need to check)

        result = [r[k] for k in self._primary_key_columns]
        return result

    #def get_primary_key_string(self, r):

    def add_rows(self,r):
        if self._rows is None:
            self._rows = {}
        rid = self.get_next_row_id()

        if self._indexes is not None:
            for k, v in self._indexes.items():
                v.add_to_index(r, rid)







        #self._rows.append(r)


    def build(self, i_name):

        idx = self._indexes[i_name]
        for k, v in self._rows.items():
            idx.add_to_index(v,k)

    def get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def insert(self, r):

        if self._rows is None:
            self._rows = {}
        rid = self.get_next_row_id()

        if self._indexes is not None:
            for k,v in self._indexes.items():
                v.add_to_index(r,rid)

        self._rows[rid] = copy.copy(r)


    def import_data(self, rows):
        for r in rows:
            self.insert(r)

    def find_by_template(self, tmp, fields, use_index=True):

        idx = self.get_best_index(tmp)
        #logging.debug("Using index = %s", idx)


        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, list(self._rows.values()))

        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        final = result
        if result is not None:
            final = []
            for r in result:
                if fields is not None:
                    finalr = {k:r[k] for k in fields}
                else:
                    finalr = r

                final.append(finalr)

        new_t = CSVDataTable(table_name="Derived:" + self._table_name, loadit=True)
        new_t.load_from_rows(table_name="Derived:" + self._table_name, rows=final)

        return new_t

    #def load_from_rows(self):

    def _find_by_template(self,tmp, fields = None, use_index=True):
        idx = self.get_best_index(tmp)
        # logging.debug("Using index = %s", idx)

        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, list(self._rows.values()))

        else:
            idx = self._indexes[idx]
            rowid = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, rowid)
            result2 = self.dictionary(result, idx._index_data)
            rows = []
            #for i in rowid:
            for j in result2.keys():

                r = self._rows[j]
                if self.matches_template(r, tmp):
                    rows.append({j:self._rows[j]})

            result = rows



        return result


    def dictionary(self, orderdict,dic):
        for i in orderdict:
            repr(dict(i))
            new = {}
            for k, v in dic.items():
                for key, value in i.items():
                    if (k == value):
                        # print('year')
                        a = int(''.join(str(e) for e in v))
                        new[a] = i
                    else:
                        # print("boo")
                        continue
        return new



    def get_best_index(self, t):

        best = None
        n = None

        if self._indexes is not None:
            for k,v in self._indexes.items():
                cnt = v.matches_index(t)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k
                    else:
                        if cnt > best:
                            best = len(v.keys())
                            n = k

        return n


    def find_by_scan_template(self,template, row):
        some_rows = []


        #row = repr(dict(row))
        for r in row:
            r = dict(r)
            if self.matches_template(template, r):
                if some_rows is None:
                    some_rows = []
                some_rows.append(r)

        return some_rows



    def find_by_index(self, tmp, idx):
        r = idx.find_rows(tmp)
        res = [self._rows[k] for k in r]
        return res

    def save(self):

        d = {
            "state": {
                "table_name": self._table_name,
                "primary_key_columns": self._primary_key_columns,
                "next_rid": self.get_next_row_id(),
                "column_names": self._column_names
            }
        }
        fn = CSVDataTable._default_directory + self._table_name + ".json"
        d["rows"] = self._rows

        for k,v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d['indexes'] = idxs

        d = json.dumps(d, indent=2)
        with open(fn, "w+") as outfile:
            outfile.write(d)

    def to_json(self):
        result = ""
        result += "Index name: " + self.name
        result += "\nKind: " + self._kind
        result += "\nIndex columns: " + str(self.index_columns)
        result += "\nTable name: " + self.table_name

        if self._index_data is not None:
            keys = list(self._index_data.keys())
            result += "\nNo. of unique index values: " + str(len(keys))
            cnt = min(5, len(keys))
            result += "Entries:\n"
            for i in range(0, cnt):
                result += "[" + keys[i] + ":" + json.dumps(self._index_data[keys[i]], indent=2) + "]\n"

        return result

    def load(self, dir):
        result = []
        cols = None
        with open(dir, "r") as infile:
            rdr = csv.DictReader(infile)
            cols = rdr.fieldnames
            for r in rdr:
                result.append(r)

        return result, cols

    def matches_template(self, temp, row):
        if temp is None:
            return True

        keys = temp.keys()
        row_keys = row.keys()
        for k in keys:
            v = row.get(k, None)
            t = temp[k]
            if k not in temp:
                continue
            elif k not in row_keys:
                continue

            elif temp[k] != v:
                return False


        return True

    # def matches_template(self, row, tmp):
    #     result = False
    #
    #     if tmp is None or tmp == {}:
    #         result = True
    #     else:
    #         for k in tmp.keys():
    #             v = row.get(k, None)
    #             if v != tmp[k]:
    #                 result = False
    #                 break
    #
    #         else:
    #             result = True
    #
    #     return result


    def load_from_rows(self, table_name, rows):
        self._table_name = table_name
        self._column_names = None
        self._indexes = None
        self._rows = {}
        self._next_row_id = 1

        for r in rows:
            if self._column_names is None:
                self._column_names = list(sorted(r.keys()))

            self.insert(r)

    def _remove_row(self, rid):
        r = self._rows[rid]
        for n, idx in self._indexes.items():
            idx.delete_from_index(r,rid)

        del[self._rows[rid]]
        print("Deleted")


    def remove_rows(self, rid):

        r = self._rows[rid]
        for n, idx in self._indexes.items():
            idx.delete_from_index(r, rid)

        del[self._rows[rid]]

    def get_rows(self):
        if self._rows is not None:
            result = []
            for k,v in self._rows.items():
                result.append(v)
        else:
            result =  None

        return result


    def remove_from_index(self, rid, row):
        print("would delete rid = ", rid, "row = ", row)
        key = self.compute_key(row)
        bucket = self._index_data.get(key, {})
        bucket.remove(rid)
        print("Bucket = ", bucket)

    #def compute_key(self, row):

    def join(self, right_table, on_clause, where_clause, fields, optimize = True):

        if optimize:
            probe_table, scan_table = self._get_scan_probe(self, right_table, on_clause)

        else:
            scan_table = self
            probe_table = right_table


        if scan_table != self and optimize:
            logging.debug("Swapping tables")
        else:
            logging.debug("Not swapping tables")

        logging.debug("Before pushdown, scan rows = %s", len(scan_table.get_rows()))

        if optimize:
            scan_template = scan_table._get_specific_where(where_clause)
            scan_projection = scan_table._get_specific_project(fields)


            scan_rows = scan_table.find_by_template(scan_template, scan_projection)
            logging.debug("After pushdown, scan rows = %s", len(scan_rows.get_rows()))
        else:
            scan_rows = scan_table

        new_scan_rows = scan_rows.get_rows()

        result = []

        for r in new_scan_rows:

            probe_where = CSVDataTable.on_clause_to_where(on_clause, r)
            probe_projection = probe_table._get_specific_project(fields)

            probe_rows = probe_table.find_by_template(probe_where,probe_projection, use_index=optimize)
            probe_rows = probe_rows.get_rows()

            if probe_rows:
                for r2 in probe_rows:
                    new_r = {**r, **r2}
                    result.append(new_r)

        tn = "Join(" + self.get_table_name() + "," + right_table.get_table_name() + ")"
        final_result = CSVDataTable(table_name=tn,loadit=True)

        final_result.load_from_rows(table_name=tn, rows=result)

        #Apply the template result table

        return final_result


    @staticmethod
    def _get_scan_probe(left_table, right_table, on_clause):

        scan_best, scan_selective = left_table.get_index_and_selectivity(on_clause)
        probe_best, probe_selective = right_table.get_index_and_selectivity(on_clause)

        result = left_table, right_table

        if scan_best is None and probe_best is None:
            result = left_table, right_table

        elif scan_best is None and scan_best is not None:
            result  = right_table, left_table

        elif scan_best is not None and scan_best is None:
            result = left_table, right_table

        elif scan_best is not None and scan_best is not None and scan_selective < probe_selective:
            result = right_table, left_table

        return result

    def get_index_and_selectivity(self, columns):

        on_template = dict(zip(columns, [None] * len(columns)))
        best = None
        n = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows)) / (self._indexes[n].get_no_of_entries())

        return n, best

    def get_best_index(self, temp):

        best = None
        n = None

        if self._indexes is not None:

            for k,v in self._indexes.items():
                cnt = v.matches_index(temp)

                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k

                    else:
                        if cnt > best:
                            best = len(v.get_no_of_entries())
                            n = k

        return n


    def _get_specific_where(self, w_clause):

        result = {}
        if w_clause is not None:
            for k, v in w_clause.items():
                kk = k.split(".")
                if len(kk) == 1:
                    result[k] = v
                elif kk[0] == self._table_name:
                    result[kk[1]] = v

        if result == {}:
            result = None

        return result


    @staticmethod
    def on_clause_to_where(on_clause, row):

        result = {c:row[c] for c in on_clause}
        return result

    def _get_specific_project(self, fields):
        result = []
        if fields is not None:
            for k in fields:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])

        if result == []:
            result = None

        return result



    def delete(self, temp):
        result = self._find_by_template(temp)
        rows = result

        if rows is not None:
            for i in rows:

                for k in i.keys():
                    self._remove_row(k)
