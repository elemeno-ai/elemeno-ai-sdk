VERSION 0.6
FROM amancevice/pandas:1.4.0

build:
    ARG version
    RUN pip install elemeno-ai-sdk=$version --extra-index-url http://10.249.3.245:8080/simple --trusted-host 10.249.3.245
    SAVE IMAGE sdk-debug:latest

