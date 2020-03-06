FROM python:3.6-slim

RUN apt update \
    && apt install -y git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install pipenv

# we get a copy of geocoder_tester
ADD https://api.github.com/repos/QwantResearch/geocoder-tester/git/refs/heads/master geocoder-tester_version.json
RUN git clone -b master https://github.com/QwantResearch/geocoder-tester.git /app/geocoder_tester

WORKDIR /app/runner

ADD Pipfile* ./

RUN pipenv install --system --deploy

ADD invoke.yaml \
    tasks.py \
    ./

# To prevent `print()` buffering
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "invoke" ]