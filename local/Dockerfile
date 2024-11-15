FROM python:3.10-slim

# This command is for webhooks support
RUN apt-get update && apt-get install -y autoconf automake libtool make python3-dev

WORKDIR /code

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r /code/requirements.txt

RUN pip install debugpy

COPY ./app app/

# Expose debugpy port too
EXPOSE 8080 5678

CMD ["python", "-m", "debugpy", "--listen", "0.0.0.0:5678", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]

