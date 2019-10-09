# input params
# path to the config file, see pullStatsConfig.json

import boto3, json, datetime, sys
from collections import defaultdict

def getCmdMetrics():
    metrics = [
        'GetTypeCmds', 'HashBasedCmds','HyperLogLogBasedCmds', 'KeyBasedCmds', 'ListBasedCmds', 'SetBasedCmds', 
        'SetTypeCmds', 'SortedSetBasedCmds', 'StringBasedCmds', 'StreamBasedCmds']
    return metrics

def getMetrics():
    metrics = [
        'CurrItems', 'BytesUsedForCache', 'CacheHits', 'CacheMisses', 'CurrConnections',
        'NetworkBytesIn', 'NetworkBytesOut', 'NetworkPacketsIn', 'NetworkPacketsOut', 
        'EngineCPUUtilization', 'Evictions', 'ReplicationBytes', 'ReplicationLag',]
    return metrics

def calc_expiry_time(expiry):
    """Calculate the number of days until the reserved instance expires.
    Args:
        expiry (DateTime): A timezone-aware DateTime object of the date when
            the reserved instance will expire.
    Returns:
        The number of days between the expiration date and now.
    """
    return (expiry.replace(tzinfo=None) - datetime.datetime.utcnow()).days

def getClustersInfo(session):
    """Calculate the running/reserved instances in ElastiCache.
    Args:
        session (:boto3:session.Session): The authenticated boto3 session.
    Returns:
        A dictionary of the running/reserved instances for ElastiCache nodes.
    """
    conn = session.client('elasticache')
    results = {
        'elc_running_instances': {},
        'elc_reserved_instances': {},
    }
    
    paginator = conn.get_paginator('describe_cache_clusters')
    page_iterator = paginator.paginate(ShowCacheNodeInfo=True)
    # Loop through running ElastiCache instance and record their engine,
    # type, and name.
    for page in page_iterator:
        for instance in page['CacheClusters']:
            if instance['CacheClusterStatus'] == 'available' and instance['Engine'] == 'redis':
                clusterId = instance['CacheClusterId']

                results['elc_running_instances'][clusterId] = instance

    paginator = conn.get_paginator('describe_reserved_cache_nodes')
    page_iterator = paginator.paginate()

    # Loop through active ElastiCache RIs and record their type and engine.
    for page in page_iterator:
        for reserved_instance in page['ReservedCacheNodes']:
            if reserved_instance['State'] == 'active' and reserved_instance['ProductDescription'] == 'redis':
                instance_type = reserved_instance['CacheNodeType']

                # No end datetime is returned, so calculate from 'StartTime'
                # (a `DateTime`) and 'Duration' in seconds (integer)
                expiry_time = reserved_instance[
                    'StartTime'] + datetime.timedelta(
                        seconds=reserved_instance['Duration'])
                results['elc_reserved_instances'][(instance_type)] = {
                    'count': reserved_instance['CacheNodeCount'],
                    'expiry_time': calc_expiry_time(expiry=expiry_time)
                }

    return results    

def writeCmdMetric(clusterId, node, metric):
    """Write Redis commands metrics to the file
    Args:
        ClusterId, node and metric to write
    Returns:
    """
    response = cw.get_metric_statistics(
        Namespace='AWS/ElastiCache',
        MetricName=metric,
        Dimensions=[
            {'Name': 'CacheClusterId', 'Value': clusterId},
            {'Name': 'CacheNodeId', 'Value': node}
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),
        EndTime=datetime.datetime.now().isoformat(),
        Period=3600,
        Statistics=['Maximum']
    )

    max = 0
    for rec in response['Datapoints']:
        if(rec['Maximum'] > max):
            max = rec['Maximum']
    
    f.write("%s," % max)
    
def writeMetric(clusterId, node, metric):
    """Write node related metrics to file
    Args:
        ClusterId, node and metric to write
    Returns:
    """
    response = cw.get_metric_statistics(
        Namespace='AWS/ElastiCache',
        MetricName=metric,
        Dimensions=[
            {'Name': 'CacheClusterId', 'Value': clusterId},
            {'Name': 'CacheNodeId', 'Value': node}
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),
        EndTime=datetime.datetime.now().isoformat(),
        Period=3600,
        Statistics=['Maximum']
    )

    max = 0
    for rec in response['Datapoints']:
        if(rec['Maximum'] > max):
            max = rec['Maximum']
    
    f.write("%s," % max)

def writeHeaders():
    """Write file headers to the csv file
    Args:
    Returns:
    """
    f.write('ClusterId,NodeId,NodeType,Region,')
    for metric in getMetrics():
        f.write('%s (max over last week),' % metric)
    for metric in getCmdMetrics():
        f.write('%s (peak last week / hour),' % metric)
    f.write("\r\n")

def writeClusterInfo(clustersInfo):
    """Write all the data gathered to the file
    Args:
        The cluster information dictionary
    Returns:
    """
    for instanceId, instanceDetails in clustersInfo['elc_running_instances'].items():
        for node in instanceDetails.get('CacheNodes'):
            print("Getting node % s details" %(instanceDetails['CacheClusterId']))
            if 'ReplicationGroupId' in instanceDetails:
                f.write("%s," % instanceDetails['ReplicationGroupId'])
            else:
                f.write(",")

            f.write("%s," % instanceId)
            f.write("%s," % instanceDetails['CacheNodeType'])
            f.write("%s," % instanceDetails['PreferredAvailabilityZone'])
            for metric in getMetrics():
                writeMetric(instanceId, node.get('CacheNodeId'), metric)
            for metric in getCmdMetrics():
                writeCmdMetric(instanceId, node.get('CacheNodeId'), metric)
            f.write("\r\n")

    f.write("\r\n")
    f.write("\r\n")
    f.write("###Reserved Instances")
    f.write("\r\n")
    f.write("Instance Type, Count, Remaining Time (days)")
    f.write("\r\n")
    
    for instanceId, instanceDetails in clustersInfo['elc_reserved_instances'].items():
        f.write("%s," % instanceId)
        f.write("%s," % instanceDetails['count'])
        f.write("%s," % instanceDetails['expiry_time'])
        f.write("\r\n")


customer = "customerEC"
f= open("%s.csv" % customer,"w+")
with open(sys.argv[1]) as config_file:
    inputParams = json.load(config_file)

accessKey = inputParams['accessKey']
secretKey = inputParams['secretKey']
region = inputParams['region']

# connect to ec 
# aws key, secret and region
session = boto3.Session(
    aws_access_key_id=accessKey, 
    aws_secret_access_key=secretKey,
    region_name=region)

cw = session.client('cloudwatch')
pr = session.client('ce')
now = datetime.datetime.now()
start = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
end = now.strftime('%Y-%m-%d')
pricingData = pr.get_cost_and_usage(TimePeriod={'Start': start, 'End':  end}, 
    Granularity='MONTHLY',
    Filter={"Dimensions": {'Key': 'SERVICE', 'Values':['Amazon ElastiCache']}}, 
    Metrics=['UnblendedCost'])

costs = 0
for res in pricingData['ResultsByTime']:
    costs = costs + float(res['Total']['UnblendedCost']['Amount'])    

print('Grab a coffee this script takes a while...')
print('Writing Headers')
writeHeaders()
print('Gathring data...')
clustersInfo = getClustersInfo(session)
writeClusterInfo(clustersInfo)
f.write("\r\n")
f.write("####Total costs per month####  %s" % costs)
print('###Done###')


