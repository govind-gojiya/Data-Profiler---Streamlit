import streamlit as st
from sqlalchemy import create_engine, inspect, text, Column, Integer, String, select, update, Table, MetaData
from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from Tabs import metrics_calculation as mtcal
import time
from Tabs.engine_create_snowflake import Engine_Creater

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

__table_name_for_crud__ = ""
metadata = MetaData()
__data_for_crud__ = None

def update_metrics_db(table_name=__table_name_for_crud__, data=__data_for_crud__):
    engine_object = Engine_Creater()
    session = engine_object.session
    if not table_name:
        table_name = __table_name_for_crud__
    if not data:
        data = __data_for_crud__
    try:
        check_exsits_query = select(Metrics).where(Metrics.Tablename == table_name)
        is_table_exists = session.execute(check_exsits_query).all()
        
        try :
            if len(is_table_exists) == 0:
                metrics = Metrics(Tablename=__table_name_for_crud__, Rowcount=mtcal.row_count(data), Columncount=mtcal.col_count(data), Duplicates=mtcal.duplicates_count(data), Nulls=mtcal.nulls_count(data), Centraltendency=mtcal.central_tendancy_count(data))
                session.add(metrics)
                session.commit()
                st.toast(f'Hooray! Added metrics details successfully', icon='🎉')
                time.sleep(0.5)
            else:
                metrics_update_query = update(Metrics).where(Metrics.Tablename.in_([__table_name_for_crud__])).values(Rowcount=mtcal.row_count(data), Columncount=mtcal.col_count(data), Duplicates=mtcal.duplicates_count(data), Nulls=mtcal.nulls_count(data), Centraltendency=mtcal.central_tendancy_count(data))
                session.execute(metrics_update_query)
                session.commit()
                st.toast(f'Hooray! Updated metrics details successfully', icon='🎉')
                time.sleep(0.5)
        except Exception as e:
            session.rollback()
            print("Error while insertion/updation of metrics !!!\nError: ", e)
        
    except Exception as e:
        session.rollback()
        print("Error while searching metrics data: ", e)

def get_data_from_table_crud(table_name = __table_name_for_crud__):
    if table_name is not None and table_name != "":
        global __table_name_for_crud__
        __table_name_for_crud__ = table_name

    try:
        engine_object = Engine_Creater()
        global __data_for_crud__
        table = Table(__table_name_for_crud__, MetaData(), autoload_with=engine_object.engine)
        allDataDB = select(table)
        allDataDB = engine_object.session.execute(allDataDB).all()
        __data_for_crud__ = pd.DataFrame(allDataDB)

        if len(__data_for_crud__.columns) == 0:
            allDataDB = table.columns.items()
            table_cols = []
            for col_names, _ in allDataDB:
                table_cols.append(col_names)
            __data_for_crud__ = pd.DataFrame(None, columns=table_cols)
        
        table_primary_col = table.primary_key.columns.values()[0].name
        __data_for_crud__ = __data_for_crud__.drop(table_primary_col, axis=1, errors="ignore")

    except Exception as e:
        print("Error in fetching details !!!", " \nError: ", e)

def view_data_ui(tab):
    tab.title("View Data")
    view_data_btn = tab.button("View Data", use_container_width=True)
    if view_data_btn:
        tab.button("Close table")
        get_data_from_table_crud()
        tab.table(__data_for_crud__)

def add_data_to_db(newRecord):
    insertion_query = f"INSERT INTO {__table_name_for_crud__} ("
    query_columns = ""
    query_values = ""
    is_first = True
    for col_name, col_value in newRecord.items():
        if len(col_value)>0:
            if is_first:
                query_columns += f" {col_name}"
                query_values += f" '{col_value}'"
                is_first = False
                continue
            query_columns += f", {col_name}"
            query_values += f", '{col_value}'"
    insertion_query += query_columns + " ) VALUES( " + query_values + " );"
    engine = Engine_Creater()
    session = engine.session
    try:
        session.execute(text(insertion_query))
        session.commit()
        st.toast(f'Hooray! Added row successfully', icon='🎉')
        time.sleep(0.5)
        get_data_from_table_crud()
        update_metrics_db()
    except Exception as e:
        session.rollback()
        print("Error while inserting data: ", e)

def insert_data_ui(tab):
    tab.title("Insert Data: ")
    newRecord = {}
    left_container, right_container = tab.columns(2)
    half = len(__data_for_crud__.columns) // 2
    for i in range(len(__data_for_crud__.columns)):
        if i <= half:
            newRecord.update({__data_for_crud__.columns[i]: left_container.text_input(__data_for_crud__.columns[i], key=f"crud_col_value{i}")})
        else :
            newRecord.update({__data_for_crud__.columns[i]: right_container.text_input(__data_for_crud__.columns[i], key=f"crud_col_value{i}")})
    addRecordBtn = tab.button("Add Data", type="primary", use_container_width=True)
    if addRecordBtn:
        add_data_to_db(newRecord)

def update_data_to_db(**updateRecord):
    updation_query = f"UPDATE {__table_name_for_crud__} SET"
    is_first_entry = True
    for k, v in updateRecord.items():
        if k != 'column_name_to_update' and k != updateRecord.get('column_name_to_update'):
            if is_first_entry:
                updation_query += f" {k} = '{v}'"
                is_first_entry = False
                continue
            else:
                updation_query += f", {k} = '{v}'"
    updation_query += f" WHERE { updateRecord.get('column_name_to_update') } = '{updateRecord.get(updateRecord.get('column_name_to_update'))}';"
    engine = Engine_Creater()
    session = engine.session
    try:
        session.execute(text(updation_query))
        session.commit()
        st.toast(f'Hooray! Updated data successfully', icon='🎉')
        time.sleep(0.5)
        get_data_from_table_crud()
        update_metrics_db()
    except Exception as e:
        print(e)
        session.rollback() 

def update_data_ui(tab):
    tab.title("Update Data: ")
    select_attr_update = tab.selectbox("Select Column to update details:", __data_for_crud__.columns, key="updateData")
    value_for_given_attr = __data_for_crud__[select_attr_update].tolist()
    selectedValueToUpdate = tab.selectbox("Select value you want to change: ", value_for_given_attr, key="findallvaluestoupdate", index=None)

    if selectedValueToUpdate:
        updateRecord = __data_for_crud__.loc[__data_for_crud__[st.session_state.updateData] == st.session_state.findallvaluestoupdate].iloc[0:1,:].to_dict(orient='records')[0]
        updateRecord.update({"column_name_to_update": st.session_state.updateData})
        left_container, right_container = tab.columns(2)
        half = len(__data_for_crud__.columns) // 2
        for i in range(len(__data_for_crud__.columns)):
            if i <= half:
                updateRecord.update({__data_for_crud__.columns[i]: left_container.text_input(__data_for_crud__.columns[i], value=updateRecord[__data_for_crud__.columns[i]], key=f"crud_col_value_update_{i}")})
            else :
                updateRecord.update({__data_for_crud__.columns[i]: right_container.text_input(__data_for_crud__.columns[i], value=updateRecord[__data_for_crud__.columns[i]], key=f"crud_col_value_update_{i}")})
        updateRecordBtn = tab.button("Update Data", type="primary", use_container_width=True, kwargs=updateRecord, on_click=update_data_to_db)

def delete_data_from_db(**deleteDict):
    delete_record_query = f"DELETE FROM {__table_name_for_crud__} WHERE "
    for k, v in deleteDict.items():
        delete_record_query += f"{k} = '{v}'"
    engine_object = Engine_Creater()
    session = engine_object.session
    try:
        session.execute(text(delete_record_query))
        session.commit()
        st.toast(f'Hooray! Deleted data successfully', icon='🎉')
        time.sleep(0.5)
        get_data_from_table_crud()
        update_metrics_db()
    except Exception as e:
        print("Error while deleting data !!! \nError", e)
        session.rollback()

def delete_data_ui(tab):
    tab.title("Delete Data: ")
    delete_col_name = tab.selectbox("Select any attribute from which you want to delete:", __data_for_crud__.columns, key="deleteAttr")
    setAttrDelete = __data_for_crud__.iloc[0:][st.session_state.deleteAttr].tolist()
    delete_col_value = tab.selectbox("Select data you want to delete:", setAttrDelete, key="deleteTrainee", index=None)

    if delete_col_value:
        where_clause_to_delete = dict({delete_col_name: delete_col_value})
        tab.table(__data_for_crud__.loc[__data_for_crud__[delete_col_name] == delete_col_value])
        tab.button("Delete Records", type="primary", use_container_width=True, kwargs=where_clause_to_delete, on_click=delete_data_from_db)        

def main(tab):
    
    selected_table_crud = tab.selectbox("Select table for crud: ", [x for x in list(inspect(Engine_Creater().engine).get_table_names()) if x != "Metrics"], index=None)
    # selected_table_crud = select_table_from_db(tab)
    if selected_table_crud != None:
        get_data_from_table_crud(selected_table_crud)
        view_data_ui(tab)
        insert_data_ui(tab)
        update_data_ui(tab)
        delete_data_ui(tab)
    