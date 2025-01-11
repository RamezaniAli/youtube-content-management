FROM apache/airflow:2.10.4

COPY requirements.txt /
RUN cat /requirements.txt  # To verify the contents are copied correctly
RUN pip install -r /requirements.txt
