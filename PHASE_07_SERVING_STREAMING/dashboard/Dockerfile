FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY kpi_dashboard.py /app/kpi_dashboard.py

EXPOSE 8501

CMD ["streamlit", "run", "/app/kpi_dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]
