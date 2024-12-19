import streamlit as st
from sqlalchemy import create_engine, inspect, MetaData, text, Column, Integer as sqlalchemyInteger, Float as sqlalchemyFloat, Boolean as sqlalchemyBoolean, Date as sqlalchemyDate, DateTime as sqlalchemyDateTime, SmallInteger as sqlalchemySmallInteger, String as sqlalchemyString, Text as sqlalchemyText, Time as sqlalchemyTime
from sqlalchemy.orm import sessionmaker, Session 
from Tabs import metrics_calculation as mtcal
from sqlalchemy.ext.declarative import declarative_base
import time
import os.path
import pandas as pd
from Tabs.engine_create_snowflake import Engine_Creater


__defindDatatypes__ = ["Boolean", "Date", "DateTime", "Float", "Integer", "SmallInteger", "String", "Text", "Time"]
__defindSQLDatatypes__ = ["Boolean", "DATE", "DATETIME", "FLOAT", "INTEGER", "SMALLINT", "VARCHAR(255)", "TEXT", "TIME"]
if "is_generating_schema" not in st.session_state:
        st.session_state.is_generating_schema = False

engine_object = Engine_Creater()
engine = engine_object.engine
session = engine_object.session

Base = declarative_base()
# class Base(DeclarativeBase):
#     pass

class Metrics(Base):
    __tablename__ = "Metrics"
    ID = Column(sqlalchemyInteger, primary_key=True, autoincrement=True)
    Tablename = Column(sqlalchemyString(255))
    Rowcount = Column(sqlalchemyInteger)
    Columncount = Column(sqlalchemyInteger)
    Duplicates = Column(sqlalchemyString(255))
    Nulls = Column(sqlalchemyString(255))
    Centraltendency = Column(sqlalchemyString(255))

def create_metrics_table():
    if not inspect(engine).has_table("Metrics"):
        Base.metadata.create_all(bind=engine)

def update_metrics_db(table_name, data):
    try :
        metrics = Metrics(Tablename=table_name, Rowcount=mtcal.row_count(data), Columncount=mtcal.col_count(data), Duplicates=mtcal.duplicates_count(data), Nulls=mtcal.nulls_count(data), Centraltendency=mtcal.central_tendancy_count(data))
        session.add(metrics)
        session.commit()
        st.toast(f'Hooray! Added metrics details successfully', icon='🎉')
        time.sleep(0.5)
    except Exception as e:
        session.rollback()
        print("Error while insertion/updation of metrics !!!\nError: ", e)

def set_file_col_type(tab, data):
    newType = {}
    left_container, right_container = tab.columns(2)
    half = int (len(data.columns) / 2)
    data_dtypes = {}
    for i,j in zip(data.columns, data.dtypes):
        if "object" in str(j):
            data_dtypes[i] = "String"
                                 
        if "datetime" in str(j):
            data_dtypes[i] = "DateTime"

        if "float" in str(j):
            data_dtypes[i] = "Float"

        if "int" in str(j):
            data_dtypes[i] = "Integer"

        if "boolean" in str(j):
            data_dtypes[i] = "Boolean"
    
    for i in range(len(data.columns)):
        if i <= half:
            newType.update({data.columns[i]: left_container.selectbox(f"Select datatype for column '{data.columns[i]}' :", __defindDatatypes__, index=__defindDatatypes__.index(data_dtypes[data.columns[i]]))})
        else :
            newType.update({data.columns[i]: right_container.selectbox(f"Select datatype for column '{data.columns[i]}' :", __defindDatatypes__, index=__defindDatatypes__.index(data_dtypes[data.columns[i]]))})
    changeTypeBtn = tab.button("Change Type", type="primary", use_container_width=True)
    return newType

def map_toSQLAlchemy_type(tableDataType):
    dtypedict = {}
    for col_name, col_type in tableDataType.items():
        if "Boolean" == col_type:
            dtypedict.update({col_name: sqlalchemyBoolean})

        if "Date" == col_type:
            dtypedict.update({col_name: sqlalchemyDate})

        if "DateTime" == col_type:
            dtypedict.update({col_name: sqlalchemyDateTime})

        if "Time" == col_type:
            dtypedict.update({col_name: sqlalchemyTime})

        if "String" == col_type:
            dtypedict.update({col_name: sqlalchemyString(length=255)})

        if "Text" == col_type:
            dtypedict.update({col_name: sqlalchemyText})

        if "datetime" == col_type:
            dtypedict.update({col_name: sqlalchemyDateTime})

        if "Float" == col_type:
            dtypedict.update({col_name: sqlalchemyFloat(2)})

        if "Integer" == col_type:
            dtypedict.update({col_name: sqlalchemyInteger})

        if "SmallInteger" == col_type:
            dtypedict.update({col_name: sqlalchemySmallInteger})
    
    return dtypedict

def select_data_from_file(tab):
    uploaded_file = tab.file_uploader("Choose a file")
    if uploaded_file is not None:
        fileType = os.path.splitext(uploaded_file.name)[1][1:].strip().lower()
        data = None
        if fileType in ["csv", "txt"]:
            try:
                data = pd.read_csv(uploaded_file)
            except:
                try:
                    data = pd.read_csv(uploaded_file, delimiter="\t")
                except Exception as e:
                    print("Error in reading !!! Error: ", e)
        if fileType in ["xls", "xlsx", "xlsm", "xlsb", "odf", "ods", "odt"]:
            try: 
                data = pd.read_excel(uploaded_file)
            except Exception as e:
                    print("Error in reading !!! Error: ", e)
        data_dtypes = set_file_col_type(tab, data)
        table_name = tab.text_input("Give name of table: ")
        primary_col = tab.selectbox("Select Column which you want as primary key: ", data.columns)
        isAddingFile = tab.button("Store Data", use_container_width=True)
        if isAddingFile:
            mapped_dtypes = map_toSQLAlchemy_type(data_dtypes)
            try:
                addFileData = data.to_sql(name=table_name, con=engine, index=False, if_exists='replace', dtype=mapped_dtypes)
                # engine_refresh_object = Engine_Creater()
                # engine_refresh = engine_refresh_object.engine
                # with engine_refresh.connect() as con:
                #     # con.execute(text(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`{primary_col}`)"))   ########## This is for mysql not for snowflake
                #     # con.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_col})"))    ############ This is for snowflake
                if addFileData:
                    st.toast('Hooray! Created Successfully', icon='🎉')
                    time.sleep(0.5)
                    update_metrics_db(table_name, data)
            except Exception as e:
                st.toast("Something went wrong !")
                print("Error in file inserting to database ! \nError: ", e)

def create_table_on_manual(datatypes, primary_col, name):
    create_table_query = f"CREATE TABLE {name}("
    is_first = True
    for k, v in datatypes.items():
        if is_first:
            create_table_query += f"{k} {__defindSQLDatatypes__[__defindDatatypes__.index(v)]}"
            is_first = False
        else:
            create_table_query += f", {k} {__defindSQLDatatypes__[__defindDatatypes__.index(v)]}"
        if k == primary_col:
            create_table_query += " AUTO_INCREMENT"
    create_table_query += f", PRIMARY KEY ({primary_col}))"
    try:
        session.execute(text(create_table_query))
        session.commit()
        st.toast(f'Hooray! Created Successfully', icon='🎉')
        time.sleep(0.5)
    except Exception as e:
        print(type(str(e)), "   see the type of it")
        print("Error: ", e)
        if "already exists" in str(e):
            st.toast("With same name table alread exsits!!!")
            time.sleep(0.5)
        session.rollback()


def select_data_from_manual(tab):
    with tab.container(border=True):
        if "is_generating_schema" not in st.session_state:
            st.session_state.is_generating_schema = False
        name = tab.text_input("Provide Name Of Table: ")
        no_columns = tab.number_input("Enter Minimum number of column containing null values you want: ", min_value=1, format="%d")
        generate_schema = tab.button("Generate Structure", use_container_width=True)
        if generate_schema:
            st.session_state.is_generating_schema = True
        
        add_data_btn = None
        if st.session_state.is_generating_schema:
            columns_name = []
            types_of_col = []
            left_col_name, right_col_type = tab.columns(2) 
            for i in range(no_columns):
                columns_name.append(left_col_name.text_input("Enter name of column: ", key=f'column_name_no_{i+1}'))
                types_of_col.append(right_col_type.selectbox(f"Select datatype for {columns_name[i]}", __defindDatatypes__, key=f'column_type_no_{i+1}'))
            primary_col = tab.selectbox("Select Primary Column: ", columns_name)
            add_data_btn = tab.button("Save Data", use_container_width=True, key=add_data_btn)
        if add_data_btn:
            st.session_state.is_generating_schema = None
            datatypes = {x: y for x, y in zip(columns_name, types_of_col)}
            create_table_on_manual(datatypes, primary_col, name)

def main(tab):
    create_metrics_table()
    selected_data_option = tab.radio("Choose one option to provide data : ", ["Upload File", "Manually enter"], horizontal=True)
    if selected_data_option == "Upload File":
        select_data_from_file(tab)
    if selected_data_option == "Manually enter":
        select_data_from_manual(tab=tab)
