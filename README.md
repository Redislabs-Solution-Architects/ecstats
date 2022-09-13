# Deprecated
This repositiry is deprecated. You can pull raw usage data from Elasticache using [this](https://github.com/Redislabs-Solution-Architects/ecstats2) repository and from Redis Open Source cluster using [this](https://github.com/Redislabs-Solution-Architects/osstats) repository, respectively.
#

# ecstats
`ecstats` is the collective name of scripts (`pullElasticCacheStats.py`, `pullAzureCacheForRedisStats.py`, `pullRedisOpenSourceStats.py`) which will pull raw usage data from ElastiCache, Azure Cache for Redis and a Redis Open Source cluster, respectively.

The output from these scripts are then analyzed using the tools in the (private to Redislabs) EC2RL-Internal toolset.

# Assumptions
We assume you have the appropriate CLI installed ([AWS](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html), [Azure](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)) for your cloud provider and have appropriate authorization and authentication, or know the host/port/password if connecting to an OSS cluster.

The easiest way forward is if you have [Docker installed](https://docs.docker.com/get-docker/).

For Azure, having [jq](https://stedolan.github.io/jq/) installed is helpful too (although if you've got Docker we'll assume you can use the [jq image for docker](https://hub.docker.com/r/stedolan/jq))

# Operation
The simplest mechanism is to use the [ecstats Docker image](docker.pkg.github.com/redislabs-solution-architects/ecstats/ecstats:latest).

Each script is a little different:

## `pullElastiCacheStats.py`
Executing this command is the default command in the docker file. 

### Copy config file and edit
```
$ cp config.cfg.example config.cfg
```

### Run docker
```
$ docker run -v $PWD:/ecstats docker.pkg.github.com/redislabs-solution-architects/ecstats/ecstats:latest
```
Note that we mount the current directory onto `/ecstats`. This name `ecstats` is built into the default CMD, so go with it!

Results will be stored in the mounted folder. For each section in the config.cfg` file there'll be a an Excel file generated in the `/ecstats` directory (you did remember to mount that onto your current directory, didn't you?!)

### Running from source

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

## `pullAzureCacheForRedis`
The output will be in a file called `AzureStats.xlsx` in the current directory.

### Docker
You need to authenticate to Azure and setup an MSI first, before running the actual script. I've automated as much of this as possible, but if you don't want to run `jq` then simply run the `az ad ...` part of the command and create the necessary envars by hand.

```
az login
$(az ad sp create-for-rbac --name http://pullAzureStats  | docker run -i stedolan/jq -r '"export AZURE_CLIENT_ID=" + .appId + " AZURE_CLIENT_SECRET=" + .password + " AZURE_TENANT_ID=" +.tenant')
docker run -e AZURE_TENANT_ID -e AZURE_CLIENT_ID -e AZURE_CLIENT_SECRET -v$(pwd):/ecstats docker.pkg.github.com/redislabs-solution-architects/ecstats/ecstats:latest python /app/pullAzureCacheForRedisStats.py -d /ecstats
```

The second line above sets up all the necessary envars.

The `-d /ecstats` argument in the third line sets up the output directory to the directory mounted onto the current directory (so the output file will be available afterwards!)

### Python
Follow the instructions for the virtual environment above, then execute the following:

```
az login
$(az ad sp create-for-rbac --name http://pullAzureStats  | jq -r '"export AZURE_CLIENT_ID=" + .appId + " AZURE_CLIENT_SECRET=" + .password + " AZURE_TENANT_ID=" +.tenant')
python pullAzureCacheForRedisStats.py
```

## `pullRedisOpenSourceStats`
The script extracts the current cluster usage by using the INFO and INFO COMMANDSTATS commands.
These two Redis commands are called twice, in order to measure in order to capture the commands process during this timeframe. 

The script takes as an input the following:
- an Excel files with the Redis DB access configuration
The Excel contains the following columns:

DB Name - a logical name for the Redis DB
Redis Host - the host/IP address of the DB
Port - DB port
Password - the DB password
User (ACL) - a username to connect with to the DB which has privileges to the INFO command

A template can be found in: `samples/sampleOSSPullInput.xlsx`.

### Docker
```
$ docker run -v $PWD:/ecstats docker.pkg.github.com/redislabs-solution-architects/ec2rl-internal/ec2rl-internal:latest python pullRedisOpenSourceStats.py /ecstats/sampleOSSPullInput.xlsx
```

### Python

```
python pullRedisOpenSourceStats.py sampleOSSPullInput.xlsx
```

The output will be an Excel file with all the information gathered from the clusters. An example can be found in `samples/sampleOSSStats.xlsx`.

