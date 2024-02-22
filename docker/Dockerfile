FROM python:3.8-slim

WORKDIR /code

COPY requirements.txt .
RUN pip install -r /code/requirements.txt
COPY ./app app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
