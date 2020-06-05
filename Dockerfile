FROM python:3.8-buster

RUN apt-get update && apt-get install -y \
    poppler-utils \
    tesseract-ocr

ADD requirements.txt .
RUN pip install -r requirements.txt

RUN pip install git+https://github.com/afparsons/doccano_api_client.git@master#egg=doccano_api_client