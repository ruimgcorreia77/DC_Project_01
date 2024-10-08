FROM python:3.9

WORKDIR /marketstack

COPY /marketstack .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "marketstack.pipelines.marketstack"]
