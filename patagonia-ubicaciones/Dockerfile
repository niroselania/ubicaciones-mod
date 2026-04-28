FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY completar_ubicaciones.py /app/completar_ubicaciones.py

CMD ["python", "/app/completar_ubicaciones.py", "--watch", "--dir", "/data", "--interval-s", "3"]

