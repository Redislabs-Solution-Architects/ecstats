EC2RL
=====

EC2RL is a script which helps you migrate form AWS ElastiCache to Redis Enterprise.
The scripts extract your current usage in ElastiCache, in order to more accurately calculate your dataset and throughput.
According to this information you can then plan a Redis Enterprise Cluster to address your current needs.

## How it works
This pullElasticCacheStats script connects to your AWS account using boto3 (AWS API), and pulls out your current ElastiCache usage.
The script pulls the stats from ElastiCache, CloudWatch and Cost Estimator API's for a a specified region.
First the ElastiCache clusters information is extracted such as number of clusters and instance types.
Then additional information is extracted from CloudWatch, such as the operations types and throughput, network utilization that are needed in order to plan a well fitted Redis Enterprise Cluster.

You can see a sample out put sampleStats.csv in the outputs folder.

## Getting Started

```
# Clone:
git clone https://github.com/Redislabs-Solution-Architects/EC2RL

# Prepare virtualenv:
cd ecmigrator
mkdir .env
virtualenv .env

# Activate virtualenv
. .env/bin/activate

# Install boto3
pip install boto3

# When finished
deactivate
```

In order to run the script just pass the path to the JSON config file

```
python pullElasticCacheStats.py pullStatsConfig.json
```

The pullStatsConfig.json should contain the following information
```
{
    "accessKey": "Your AWS Access Key",
    "secretKey": "Your AWS Secret Key",
    "outputFile": "File Name",
    "region": "AWS region for example us-east-2"
}
```

