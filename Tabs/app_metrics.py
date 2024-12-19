import streamlit as st
from sqlalchemy import create_engine, inspect, text, Column, Integer, String, select, update, Table, MetaData
from sqlalchemy.orm import Session
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy.ext.declarative import declarative_base
from Tabs import metrics_calculation as mtcal
import time
from Tabs.engine_create_snowflake import Engine_Creater

engine_obj = Engine_Creater()
engine = engine_obj.engine
session = engine_obj.session

data = None
__table_name_for_metrics__ = ""

Base = declarative_base()

class Metrics(Base):
    __tablename__ = "Metrics"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    Tablename = Column(String(255))
    Rowcount = Column(Integer)
    Columncount = Column(Integer)
    Duplicates = Column(String(255))
    Nulls = Column(String(255))
    Centraltendency = Column(String(255))

def select_table_from_db(tab):
    selected_table = tab.selectbox("Select table: ", [x for x in list(inspect(Engine_Creater().engine).get_table_names()) if x != "Metrics"])
    select_table_complete = tab.button("Select Table", type="primary", use_container_width=True)
    if select_table_complete:
        return selected_table

def get_data_from_table(table_name):
    if table_name is not None:
        get_data_query = text(f"select * from {table_name}")
        if table_name is not None:
            global __table_name_for_metrics__
            __table_name_for_metrics__ = table_name
        try:
            engine_object = Engine_Creater()
            enigne = engine_object.engine
            session = engine_object.session
            global data
            allDataDB = select(Table(table_name, MetaData(), autoload_with=engine))
            allDataDB = session.execute(allDataDB).all()
            data = pd.DataFrame(allDataDB)
            if len(data.columns) == 0:
                table = Table(table_name, MetaData(), autoload_with=engine)
                allDataDB = table.columns.items()
                table_cols = []
                for col_names, _ in allDataDB:
                    table_cols.append(col_names)
                data = pd.DataFrame(None, columns=table_cols)

        except Exception as e:
            print("Error in fetching details !!!", " \nError: ", e)

def show_data(tab):
    tab.table(data)

def rows_cols_count(tab):
    rowCount = "Rows: " + str(len(data.index))
    tab.title(rowCount)
    colCount = "Columns: " + str(len(data.columns))
    tab.title(colCount)
    tab.write("Columns names: ")
    show_text = ""
    for i in range(len(data.columns)):
        show_text += f"  {i+1}. {data.columns[i]}\n"
    tab.write(show_text)

def duplicates(tab):
    selectCols = tab.multiselect("Which columns are you want to find duplicates ?", ["All", *data.columns])
    if "All" in selectCols:
        selectCols = [i for i in data.columns if i != "id" and i != "ID" and i != "Id"]
    if len(selectCols) > 0:
        duplicateRows = data[data.duplicated(selectCols, keep=False)].sort_values(selectCols)
        tab.table(duplicateRows)

def nullValues(tab):
    minimumNull = tab.number_input("Enter Minimum number of column containing null values you want: ", min_value=1, max_value=len(data.columns), format="%d")
    if minimumNull > len(data.columns):
        minimumNull = len(data.columns)
    tab.table(data.iloc[data[(data.isnull().sum(axis=1) >= minimumNull)].index])

def stdValues(tab):
    tab.title("Standard deviation of Data : ")
    stdOfCols = data.std(numeric_only = True, skipna = True)
    stdOfCols = stdOfCols.drop(["id", "ID", "Id"], errors="ignore")
    tab.table(stdOfCols)

def graph(tab):
    tab.title("Histogram for Data: ")
    df = data.drop(["id", "ID", "Id", "Index"], axis=1, errors="ignore")
    graphtableDataFrame = df.select_dtypes(include=[np.number])
    plot_cols = tab.multiselect("Select columns to plot graph: ", [x for x in graphtableDataFrame.columns])
    plot_data_list = []
    if len(plot_cols) > 0:
        for i in plot_cols:
            plot_data_list.append(graphtableDataFrame[i].tolist())
        fig, ax = plt.subplots()
        ax.hist(plot_data_list)
        tab.pyplot(fig)

def store_metrics(tab):
    tab.write("Table Name: " + __table_name_for_metrics__)
    tab.write("Row: " + str(mtcal.row_count(data)))
    tab.write("Column: " + str(mtcal.col_count(data)))
    tab.write("Duplicates: " + str(mtcal.duplicates_count(data)))
    tab.write("Nulls: " + str(mtcal.nulls_count(data)))
    tab.write("STD: " + mtcal.central_tendancy_count(data))

def main(tab):
    selected_datatable_name = select_table_from_db(tab)
    get_data_from_table(selected_datatable_name)
    current_tab_tabs = tab.tabs(["Data", "Rows-Columns", "Duplicates", "Null", "STD", "Charts", "Stored Data"])
    if data is not None:
        show_data(tab=current_tab_tabs[0])
        rows_cols_count(tab=current_tab_tabs[1])
        duplicates(tab=current_tab_tabs[2])
        nullValues(tab=current_tab_tabs[3])
        stdValues(tab=current_tab_tabs[4])
        graph(tab=current_tab_tabs[5])
        store_metrics(tab=current_tab_tabs[6])
        