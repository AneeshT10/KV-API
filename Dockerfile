FROM python:3.8-slim
WORKDIR /app
COPY . /app
RUN pip install Flask
RUN pip install requests
EXPOSE 8090
CMD ["python3", "shard_kvs.py"]