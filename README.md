ecstats
=====

ecstats is a script which helps to capture your AWS ElastiCache usage.
The scripts extract your current usage in ElastiCache, including the information such as throughput, dataset size and operation types.

## How it works

This pullElasticCacheStats script connects to your AWS account using boto3 (AWS API), and pulls out your current ElastiCache usage.
The script pulls the stats from ElastiCache, CloudWatch and Cost Estimator API's for a a specified region.
First the ElastiCache clusters information is extracted such as number of clusters and instance types.
Then additional information is extracted from CloudWatch, such as the operations types and throughput, network utilization 
that are needed in order to plan a well fitted Redis Enterprise Cluster.

You can see a sample out put sampleStats.xlsx in the outputs folder.

## Running from Docker

# Copy config file and edit
```
$ cp config.cfg.example config.cfg
```

# Run docker mount the current directory for the docker image
```
$ docker run -v$(pwd):/ecstats docker.pkg.github.com/redislabs-solution-architects/ec2rl-internal/ec2rl-internal:latest
```

# Results will be stored in the mounted folder (example)
```
$ ls *.xlsx
production-us-east-2.csv
```

## Running from source

```
# Clone:
git clone https://github.com/Redislabs-Solution-Architects/ecstats

# Prepare virtualenv:
cd ecstats
mkdir .env
virtualenv .env

# Activate virtualenv
. .env/bin/activate

# Install necessary libraries
pip install -r requirements.txt

# When finished
deactivate
```

To run the script copy the configuration file and edit:

```
cp config.cfg.example config.cfg
```

Execute 

```
python pullElasticCacheStats.py -c config.cfg
```

or to pull cloud watch stats for a longer period run

```
python pullElasticCacheStats.py -c config.cfg --days 31
```

The output will be a CSV files named according to the sections and region which are in the config file. 
