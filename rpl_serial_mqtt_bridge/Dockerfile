FROM python:3.12-slim

RUN pip install --no-cache-dir pyserial paho-mqtt

WORKDIR /app
COPY app.py /app/app.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]