ARG PYTHON_VERSION=3.12
ARG APP_NAME=language-processor-api

FROM python:${PYTHON_VERSION} as builder
ARG APP_NAME

WORKDIR /code

COPY ./shared/vector.proto /code/vector.proto

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

RUN protoc --python_out=/code vector.proto


FROM python:${PYTHON_VERSION}
ARG APP_NAME

WORKDIR /app

COPY ./${APP_NAME}/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY --from=builder /code/ /app

COPY ./${APP_NAME}/src /app

EXPOSE 8000

CMD ["python", "main.py"]