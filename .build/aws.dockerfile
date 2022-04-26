FROM amazon/aws-lambda-python:3.8

ENV AGGREGATION_MODE=""
ENV LAST_FM_API_KEY=""
ENV LOGGING_LEVEL=""
ENV S3_BUCKET=""
ENV S3_BUCKET_DIR=""

WORKDIR /tmp

COPY requirements.txt requirements.txt
COPY app.py app.py

RUN pip install --no-python-version-warning --upgrade pip
RUN pip install --no-python-version-warning wheel
RUN pip install --no-python-version-warning -r requirements.txt

WORKDIR $LAMBDA_TASK_ROOT

RUN cp -rf /tmp/src/* $LAMBDA_TASK_ROOT
RUN rm -rf {.,**}/__pycache__ {.,**}/algo_trader.egg-info /tmp/*

CMD ["app.handler"]
