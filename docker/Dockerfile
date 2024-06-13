FROM python:3.10

RUN apt-get update && apt-get install -y autoconf libtool python3-dev

WORKDIR /code

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r /code/requirements.txt
COPY ./app app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
