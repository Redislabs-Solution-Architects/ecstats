FROM python:3.8-slim
WORKDIR /app
ADD requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py /app

CMD ["python", "/app/pullElasticCacheStats.py", "--config", "/ecstats/config.cfg", "--out-dir=/ecstats"]
