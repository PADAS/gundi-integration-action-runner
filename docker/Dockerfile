FROM python:3.10  AS baseimage

RUN apt-get update && apt-get install -y autoconf libtool python3-dev

# This command is for webhooks support
RUN apt-get update && apt-get install -y autoconf automake libtool make python3-dev

WORKDIR /code

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r /code/requirements.txt

FROM baseimage AS devimage

COPY ./app app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
