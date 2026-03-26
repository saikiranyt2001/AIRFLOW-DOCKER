import streamlit as st
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="airflow",
    user="airflow",
    password="airflow"
)

df = pd.read_sql("SELECT * FROM posts", conn)

st.title("📊 Airflow Data Dashboard")

st.dataframe(df)

st.bar_chart(df['id'])
