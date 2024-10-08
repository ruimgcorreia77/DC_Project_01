FROM python:3.9

WORKDIR /marketstack

ENV API_KEY_ID=d564b4a470ad5f851ffcb7f9de554cfa
ENV SERVER_NAME=data-warehouse.czsq84s08qen.eu-west-1.rds.amazonaws.com
ENV DATABASE_NAME=dw
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=mccc67CMWeYI4bEAR2ii
ENV PORT=5432
ENV LOGGING_SERVER_NAME=data-warehouse.czsq84s08qen.eu-west-1.rds.amazonaws.com
ENV LOGGING_DATABASE_NAME=dw_logging
ENV LOGGING_USERNAME=postgres
ENV LOGGING_PASSWORD=mccc67CMWeYI4bEAR2ii
ENV LOGGING_PORT=5432
ENV PYTHONPATH=/marketstack

COPY /marketstack .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "/marketstack/pipelines/marketstack.py"]
