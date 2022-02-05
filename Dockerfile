FROM amancevice/pandas:1.4.0
ARG version
RUN pip install --upgrade pip
RUN pip install elemeno-ai-sdk==$version --extra-index-url http://10.249.3.245:8080/simple --trusted-host 10.249.3.245
