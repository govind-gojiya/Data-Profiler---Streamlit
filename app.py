import streamlit as st
import pymysql
from Tabs import app_data_provider, app_crud, app_metrics

tabs = st.tabs(["Provide Data", "CRUD on data", "Matrics"])

if __name__ == "__main__":
    with tabs[0]:
        app_data_provider.main(tabs[0])
    with tabs[1]:
        app_crud.main(tabs[1])
    with tabs[2]:
        app_metrics.main(tabs[2])