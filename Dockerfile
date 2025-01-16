FROM apache/airflow:2.10.4

COPY requirements.txt /tmp/
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir -r /tmp/requirements.txt
