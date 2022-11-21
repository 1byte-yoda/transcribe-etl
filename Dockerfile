# pull official base image
FROM python:3.9.2-slim-buster

WORKDIR /usr/src/app

ENV PYTHONPATH /usr/src/app

COPY requirements.txt ./
RUN pip install -U pip

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python" ]
