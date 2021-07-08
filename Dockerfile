FROM python:latest
WORKDIR /app
COPY requirements.txt /app
COPY *.py /app
RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "/app/pullElasticCacheStats.py", "--config", "/ecstats/config.cfg", "--out-dir=/ecstats"]