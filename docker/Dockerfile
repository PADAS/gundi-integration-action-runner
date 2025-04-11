FROM python:3.10-slim AS baseimage

# This command is for webhooks support

RUN apt-get update && \
    apt-get install -y --no-install-recommends autoconf automake libtool build-essential \
    python3-dev \
    && pip install pyjq \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r /code/requirements.txt
COPY ./app app/

FROM baseimage AS devimage

COPY requirements-dev.in .
RUN pip install -r /code/requirements-dev.in

# Install debugpy for debugging
RUN pip install debugpy

# Expose debugpy port too
EXPOSE 8080 5678
CMD ["python", "-m", "debugpy", "--listen", "0.0.0.0:5678", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]

FROM baseimage AS prodimage
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

