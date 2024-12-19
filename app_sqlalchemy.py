import streamlit as st
import pymysql
import time
import pandas as pd
import re
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine("mysql+pymysql://data_profiler:data_profiler_trainees_2929@localhost/INTERN?charset=utf8mb4")
Session = sessionmaker(bind=engine)
session = Session()


Base = declarative_base()

class Trainee(Base):
    __tablename__ = "TRAINEES"
    ID = Column(Integer, primary_key=True, autoincrement=True)
    NAME = Column(String)
    TECHNOLOGY = Column(String)
    MENTOR = Column(String)
    STIPHEND = Column(Float)

# basic needed variable config
techstack = ["AI/ML", ".Net and Angular", "DevOps", "React JS", "Node JS", "Python Developer", "GO Lang"]
tableCol = ["NAME", "TECHNOLOGY", "MENTOR", "STIPHEND"]
if "isUpdating" not in st.session_state:
    st.session_state.isUpdating = False

if 'isDeleting' not in st.session_state:
    st.session_state.isdelteAttrReady = True

# adding new trainee details to database
def addIntern(**intern):
    trainee = Trainee(NAME=st.session_state.internName,TECHNOLOGY= st.session_state.internTechnology, MENTOR=st.session_state.internMentor, STIPHEND=st.session_state.internStiphend)

    try:
        session.add(trainee)
        session.commit()
        st.toast(f'Hooray! Added details successfully', icon='🎉')
        time.sleep(0.3)
    except Exception as e:
        session.rollback()
        print("Error while inserting data: ", e)

# interface(form) for adding new trainee details
with st.form("Add Trainees:", clear_on_submit=True):
    st.title('Add User :')
    left_column, right_column = st.columns(2)

    with left_column:
        name = st.text_input("Intern's Name", value="", placeholder="Govind", key="internName")
        mentor = st.text_input("Mentor Name", value="", placeholder="Hima Soni", key="internMentor")

    with right_column:
        technology = st.selectbox("Assigned Technology", techstack, key="internTechnology")
        stiphend = round(st.number_input("Stiphend", placeholder=0.00, key="internStiphend"), 2)

    st.form_submit_button("Add Details", on_click=addIntern, type="primary", use_container_width=True)

    
st.divider()

# get all intern trainees details to show
def getTrainees(trainee = ""):
    searchQuery = text(f"""SELECT NAME, TECHNOLOGY, MENTOR, STIPHEND FROM TRAINEES {trainee}""")
    try:
        
        allInternFromDB = session.execute(searchQuery)
        allIntern = pd.DataFrame(allInternFromDB, columns=tableCol)
        return allIntern
    except Exception as e:
        print("Error in fetching. trainees details !!!", " \nError: ", e)

# interface to show trainees data
with st.container(border=True):
    st.title("All Trainees")
    st.table(getTrainees())
    # getTrainees()

st.divider()


def updateTrainee():
    if st.session_state.isUpdating:
        updateTraineeSQL = text(f'UPDATE TRAINEES SET TECHNOLOGY = "{st.session_state.updateInternTechnology}", MENTOR = "{st.session_state.updateInternMentor}",  STIPHEND = "{round(st.session_state.updateInternStiphend, 2)}" WHERE NAME = "{st.session_state.updateTrainee}"')
        try:
            session.execute(updateTraineeSQL)
            session.commit()
            st.session_state.isUpdating = False
            st.toast(f'Hooray! Updated details successfully', icon='🎉')
            time.sleep(0.3)
        except Exception as e:
            print(e)
            session.rollback()
    else:
        st.toast("Not callable like this.")

def traineeDetails():
        whereCluseForOne = f"WHERE NAME = \"{st.session_state.updateTrainee}\""
        getTraineeDetails = getTrainees(whereCluseForOne)
        print(getTraineeDetails)
        st.session_state.updateTech = techstack.index(getTraineeDetails.iloc[0]["TECHNOLOGY"])
        st.session_state.updateMentor = getTraineeDetails.iloc[0]["MENTOR"]
        st.session_state.updateStiphend = getTraineeDetails.iloc[0]["STIPHEND"]


# interface for update trainee
with st.container(border=True):
    st.title("Update Trainee Details")
    allIntern = getTrainees()
    st.selectbox("Select Trainee to update details:", allIntern.iloc[0:,0:1], index=None, key="updateTrainee")
    isFinding = st.button("Find", type="primary", use_container_width=True)
    if isFinding:
        traineeDetails()
        st.session_state.isUpdating = True
    if st.session_state.isUpdating:
        mentorCol, technoCol, stiphendCol = st.columns(3)
        mentorCol.text_input("Mentor Name", value=st.session_state.updateMentor, placeholder="Hima Soni", key="updateInternMentor")
        technoCol.selectbox("Assigned Technology", techstack, index=st.session_state.updateTech, key="updateInternTechnology")
        stiphendCol.number_input("Stiphend", value=st.session_state.updateStiphend, placeholder=0.00, key="updateInternStiphend")
        st.button("Update Info", on_click=updateTrainee, type="primary", use_container_width=True)

st.divider()

def deleteTrainees(*record):
    if st.session_state.isDeleting:
        deleteValue = ''.join(record)
        deleteTraineeSQL = text(f"DELETE FROM TRAINEES WHERE {st.session_state.deleteAttr} = '{deleteValue}'")
        try:
            session.execute(deleteTraineeSQL)
            session.commit()
            st.session_state.isDeleting = False
            st.toast(f'Hooray! Deleted details successfully', icon='🎉')
            time.sleep(0.3)
        except Exception as e:
            print(e)
            session.rollback()
    else:
        st.toast("Not callable like this.")

with st.container(border=True):
    st.title("Delete Trainee Details")
    allMetaData = getTrainees("WHERE FALSE")
    st.selectbox("Select any attribute from which you want to delete:", allMetaData.columns, index=None, key="deleteAttr")
    st.button("Set Attribute", type="primary", use_container_width=True, key="isDeleteAttrSet")

    if st.session_state.isDeleteAttrSet:
        getTraineeDetails = getTrainees()
        st.session_state.isdelteAttrReady = False
        st.session_state.setAttrDelete = getTraineeDetails.iloc[0:][st.session_state.deleteAttr].tolist()
        deleteTrainee = st.write(st.session_state.setAttrDelete)
        st.text_input("Enter Index  of trainee which you want to delete:", key="deleteTrainee")

    st.button("Find Records", type="primary", use_container_width=True, key="isShowingRecord", disabled=st.session_state.isdelteAttrReady)
    
    if st.session_state.isShowingRecord:
        whereClauseToDelete = f'WHERE {st.session_state.deleteAttr} = "{st.session_state.deleteTrainee}"'
        getListTraineeDelete = getTrainees(whereClauseToDelete)
        st.table(getListTraineeDelete)
        st.session_state.isDeleting = True
        isFinalToDelete = st.button("Delete Records", type="primary", use_container_width=True, args=st.session_state.deleteTrainee, on_click=deleteTrainees)
