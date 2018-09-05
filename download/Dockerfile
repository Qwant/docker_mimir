FROM python:3.6-slim-stretch

WORKDIR /srv

RUN apt-get update && \
    apt-get install -y \
        unzip \
        curl \
        wget

RUN pip install pipenv

COPY Pipfile* ./

RUN pipenv install --system --deploy

COPY invoke.yml tasks.py ./

ENTRYPOINT ["invoke"]
