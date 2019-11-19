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
cd EC2RL
mkdir .env
virtualenv .env

# Activate virtualenv
. .env/bin/activate

# Install necessary libraries
pip install -r requirements.txt

# When finished
deactivate
```

In order to run the script configure your AWS_* environment variables and
pass the path to the JSON config file or specify all the parameters in the config file:

```
export AWS_ACCESS_KEY_ID=<ACCESS KEY ID>
export AWS_SECRET_ACCESS_KEY=<SECRET ACCESS KEY>
export AWS_DEFAULT_REGION=<REGION>
export AWS_REGION=<REGION>

python pullElasticCacheStats.py pullStatsConfig.json
```

The pullStatsConfig.json should contain the following information
```
{
    "outputFile": "File Name"
}
```

