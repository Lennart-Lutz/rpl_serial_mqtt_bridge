FROM python:3.12-slim

RUN pip install --no-cache-dir pyserial paho-mqtt

WORKDIR /app
COPY discovery.py /app/discovery.py
COPY discovery_store.py /app/discovery_store.py
COPY mqtt_client.py /app/mqtt_client.py
COPY parsers.py /app/parsers.py
COPY app.py /app/app.py
COPY run.sh /app/run.sh

RUN sed -i 's/\r$//' /app/run.sh && chmod +x /app/run.sh

CMD ["sh", "/app/run.sh"]