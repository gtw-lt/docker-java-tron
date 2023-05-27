FROM python:3.10
RUN echo hi
RUN apt-get update && apt-get -y --no-install-recommends install cron && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pymongo==4.3.3

WORKDIR /app
COPY cleanup.py /app/cleanup.py
COPY entry.sh /app/entry.sh

# run crond as main process of container
CMD ["./entry.sh"]
