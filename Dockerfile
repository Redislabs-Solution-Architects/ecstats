FROM python:2
WORKDIR /app
COPY requirements.txt /app
COPY *.py /app
RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "/app/pullElasticCacheStats.py", "--config", "/ec2rl/config.cfg", "--out-dir=/ec2rl"]