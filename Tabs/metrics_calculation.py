import pandas as pd

def row_count(data):
    return len(data.index)

def col_count(data):
    return len(data.columns)

def col_names(data):
    cols_name_string = ""
    for i in range(len(data.columns)):
        cols_name_string += f"  {i+1}. {data.columns[i]}\n"
    return cols_name_string

def duplicates_count(data, givenCols="All"):
    if "All" in givenCols:
        givenCols = [i for i in data.columns if i.lower() != "id" and i.lower() != 'index']
    return len(data[data.duplicated(givenCols)].index)

def nulls_count(data, minimumNull = 1):
    return len(data[(data.isnull().sum(axis=1) >= minimumNull)].index)

def central_tendancy_count(data):
    numeric_cols = data.std(numeric_only = True, skipna = True)
    numeric_cols_without_id = numeric_cols.drop(["id", "ID", "Id", "Index", "index"], errors="ignore")
    return str([x for x in numeric_cols_without_id])

