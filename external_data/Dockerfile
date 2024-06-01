FROM python:3.9-slim

ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG WEATHER_API_KEY

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

WORKDIR /home/external_data

COPY external_data .

RUN pip install -r requirements.txt

CMD ["python", "weather_api.py"]