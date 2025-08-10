FROM apache/airflow:2.8.1
USER root
RUN pip install gspread google-auth pandas apache-airflow[google,hive,postgres]
USER airflow
