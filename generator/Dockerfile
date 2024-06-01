FROM python:3.9-slim

ARG POSTGRES_USER
ARG POSTGRES_PASSWORD

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

WORKDIR /home/generator

RUN mkdir csv

RUN mkdir xml

COPY generator .

RUN pip install -r requirements.txt

CMD ["python", "ecomm_product_generator.py"]
CMD ["python", "ecomm_generator.py"]