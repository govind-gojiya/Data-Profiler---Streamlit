import streamlit as st
from sqlalchemy import create_engine
import pymysql
import time
import pandas as pd
import numpy as np
import re
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session
import os.path
  

engine = create_engine("mysql+pymysql://data_profiler:data_profiler_trainees_2929@localhost/INTERN?charset=utf8mb4")
connected = engine.connect()
session = Session(engine)
tableDataFrame = None
tableName = ""
tableDatatype = None
defindDatatypes = ["BigInteger","Boolean","Date","DateTime","Enum","Double","Float","Integer","Interval","LargeBinary","MatchType","Numeric","PickleType","SchemaType","SmallInteger","String","Text","Time","Unicode","UnicodeText","Uuid"]
tabs = st.tabs(["Data", "Rows", "Columns", "Duplicates", "Null", "STD", "Charts", "Store"])



if "isUploaded" not in st.session_state:
    st.session_state.isUploaded = False
    st.session_state.visiblity = "visible"

def setDataType():
    global tableDatatype
    tableDatatype = {}
    for i,j in zip(tableDataFrame.columns, tableDataFrame.dtypes):
        if "object" in str(j):
            tableDatatype[i] = "String"
                                 
        if "datetime" in str(j):
            tableDatatype[i] = "DateTime"

        if "float" in str(j):
            tableDatatype[i] = "Float"

        if "int" in str(j):
            tableDatatype[i] = "Integer"

        if "boolean" in str(j):
            tableDatatype[i] = "Boolean"

def csv_container():
    uploaded_file = st.file_uploader("Choose a file", label_visibility=st.session_state.visiblity)
    if uploaded_file is not None:
        fileType = os.path.splitext(uploaded_file.name)[1][1:].strip().lower()
        global tableDataFrame
        if fileType in ["csv", "txt"]:
            try:
                tableDataFrame = pd.read_csv(uploaded_file)
            except:
                try:
                    tableDataFrame = pd.read_csv(uploaded_file, delimiter="\t")
                except Exception as e:
                    print("Error in reading !!! Error: ", e)
                    main()
        if fileType in ["xls", "xlsx", "xlsm", "xlsb", "odf", "ods", "odt"]:
            try: 
                tableDataFrame = pd.read_excel(uploaded_file)
            except Exception as e:
                    print("Error in reading !!! Error: ", e)
                    main()
        st.session_state.isUploaded = True
        st.session_state.visiblity = "collapsed"
        setDataType()
        main(tableDataFrame)

def data(tab):
    tab.table(tableDataFrame)

def addRecord(*record):
    global tableDataFrame
    tableDataFrame.loc[len(tableDataFrame)] = record
    print(tableDataFrame)

def rows(tab):
    rowCount = "Rows: " + str(len(tableDataFrame.index))
    tab.title(rowCount)
    newRecord = []
    left_container, right_container = tab.columns(2)
    half = int (len(tableDataFrame.columns) / 2)
    for i in range(len(tableDataFrame.columns)):
        if i <= half:
            newRecord.append(left_container.text_input(tableDataFrame.columns[i]))
        else :
            newRecord.append(right_container.text_input(tableDataFrame.columns[i]))
    addRecordBtn = tab.button("Add Data", type="primary", use_container_width=True)
    if addRecordBtn:
        addRecord(*newRecord)

def changeType(**newTypes):
    global tableDatatype
    tableDatatype = newTypes.copy()
    print("New Change: ", tableDatatype)

def cols(tab):
    colCount = "Columns: " + str(len(tableDataFrame.columns))
    tab.title(colCount)
    newType = {}
    left_container, right_container = tab.columns(2)
    half = int (len(tableDataFrame.columns) / 2)
    for i in range(len(tableDataFrame.columns)):
        if i <= half:
            newType.update({tableDataFrame.columns[i]: left_container.selectbox(f"Select datatype for column '{tableDataFrame.columns[i]}' :", defindDatatypes, index=defindDatatypes.index(tableDatatype[tableDataFrame.columns[i]]))})
        else :
            newType.update({tableDataFrame.columns[i]: right_container.selectbox(f"Select datatype for column '{tableDataFrame.columns[i]}' :", defindDatatypes, index=defindDatatypes.index(tableDatatype[tableDataFrame.columns[i]]))})
    changeTypeBtn = tab.button("Change Type", type="primary", use_container_width=True)
    if changeTypeBtn:
        changeType(**newType)

def duplicates(tab):
    selectCols = tab.multiselect("Which columns are you want to find duplicates ?", ["All", *tableDataFrame.columns])
    if "All" in selectCols:
        selectCols = [i for i in tableDataFrame.columns if i != "id" and i != "ID" and i != "Id"]
    if len(selectCols) > 0:
        duplicateRows = tableDataFrame[tableDataFrame.duplicated(selectCols, keep=False)].sort_values(selectCols)
        tab.table(duplicateRows)

def nullValues(tab):
    minimumNull = tab.number_input("Enter Minimum number of column containing null values you want: ", min_value=1, max_value=len(tableDataFrame.columns), format="%d")
    if minimumNull > len(tableDataFrame.columns):
        minimumNull = len(tableDataFrame.columns)
    tab.table(tableDataFrame.iloc[tableDataFrame[(tableDataFrame.isnull().sum(axis=1) >= minimumNull)].index])

def stdValues(tab):
    tab.title("Standard deviation of Data : ")
    stdOfCols = tableDataFrame.std(numeric_only = True, skipna = True)
    stdOfCols = stdOfCols.drop(["id", "ID", "Id"], errors="ignore")
    tab.table(stdOfCols)

def graph(tab):
    tab.title("Histogram for Data: ")
    df = tableDataFrame.drop(["id", "ID", "Id"], axis=1, errors="ignore")
    graphtableDataFrame = df.select_dtypes(include=[np.number])
    for i in range(len(graphtableDataFrame.columns)):
        tab.bar_chart(graphtableDataFrame.iloc[:,i], y=graphtableDataFrame.columns[i])

def saveOnDB():
    print("Name: ", tableName)
    print("Data: ", tableDataFrame)
    print("Datatype: ", tableDatatype)


def store(tab):
    tab.title("Store your data and metrics : ")
    global tableName
    tableName = tab.text_input("Enter name of table: ")
    tab.button("Save", use_container_width=True, on_click=saveOnDB)

def main(tableDataFrame):
    if tableDataFrame is not None:
        data(tab=tabs[0])
        rows(tab=tabs[1])
        cols(tab=tabs[2])
        duplicates(tab=tabs[3])
        nullValues(tab=tabs[4])
        stdValues(tab=tabs[5])
        graph(tab=tabs[6])
        store(tab=tabs[7])
    else:
        csv_container()

if __name__ == "__main__":
    main(tableDataFrame)
