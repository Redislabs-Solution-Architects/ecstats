# -*- coding: utf-8 -*-

# input params
# path to the config file, see pullStatsConfig.json

import boto3, json, datetime, sys, os, getopt
import pandas as pd
from optparse import OptionParser

# Different versions of python 2.7 have renamed modules
try:
    from configparser import ConfigParser

except ImportError:
    from ConfigParser import ConfigParser

def getCmdMetrics():
    metrics = [
        'GetTypeCmds', 'HashBasedCmds', 'HyperLogLogBasedCmds', 'KeyBasedCmds', 'ListBasedCmds', 'SetBasedCmds',
        'SetTypeCmds', 'SortedSetBasedCmds', 'StringBasedCmds', 'StreamBasedCmds']
    return metrics


def getMetrics():
    metrics = [
        'CurrItems', 'BytesUsedForCache', 'CacheHits', 'CacheMisses', 'CurrConnections',
        'NetworkBytesIn', 'NetworkBytesOut', 'NetworkPacketsIn', 'NetworkPacketsOut',
        'EngineCPUUtilization', 'Evictions', 'ReplicationBytes', 'ReplicationLag']
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


def getCmdMetric(cloudWatch, clusterId, node, metric, options):
    """Write Redis commands metrics to the file
    Args:
        ClusterId, node and metric to write
    Returns:
    Returns the command metric
    """
    response = cloudWatch.get_metric_statistics(
        Namespace='AWS/ElastiCache',
        MetricName=metric,
        Dimensions=[
            {'Name': 'CacheClusterId', 'Value': clusterId},
            {'Name': 'CacheNodeId', 'Value': node}
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(days=int(options.statsDays))).isoformat(),
        EndTime=datetime.datetime.now().isoformat(),
        Period=3600,
        Statistics=['Maximum']
    )

    max = 0
    for rec in response['Datapoints']:
        if (rec['Maximum'] > max):
            max = rec['Maximum']

    return(max)

def getMetric(cloudWatch, clusterId, node, metric, options):
    """Write node related metrics to file
    Args:
        ClusterId, node and metric to write
    Returns:
    The metric value
    """
    response = cloudWatch.get_metric_statistics(
        Namespace='AWS/ElastiCache',
        MetricName=metric,
        Dimensions=[
            {'Name': 'CacheClusterId', 'Value': clusterId},
            {'Name': 'CacheNodeId', 'Value': node}
        ],
        StartTime=(datetime.datetime.now() - datetime.timedelta(days=int(options.statsDays))).isoformat(),
        EndTime=datetime.datetime.now().isoformat(),
        Period=3600,
        Statistics=['Maximum']
    )

    max = 0
    for rec in response['Datapoints']:
        if (rec['Maximum'] > max):
            max = rec['Maximum']

    return(max)

def createDataFrame():
    """Create an empty dataframe with headers
    Args:
    Returns:
    The newely created pandas dataframe
    """
    dfColumns = ["ClusterId","NodeId","NodeType","Region"]
    for metric in getMetrics():
        dfColumns.append(('%s (max over last week)' % metric))
    for metric in getCmdMetrics():
        dfColumns.append(('%s (peak last week / hour)' % metric))
    df = pd.DataFrame(columns=dfColumns)
    return (df)

def writeClusterInfo(df, clustersInfo, session, options):
    """Write all the data gathered to the file
    Args:
        The cluster information dictionary
    Returns:
    """
    cloudWatch = session.client('cloudwatch')
    row = []
    i = 0

    for instanceId, instanceDetails in clustersInfo['elc_running_instances'].items():
        for node in instanceDetails.get('CacheNodes'):
            print("Getting node % s details" % (instanceDetails['CacheClusterId']))
            if 'ReplicationGroupId' in instanceDetails:
                row.append("%s" % instanceDetails['ReplicationGroupId'])
            else:
                row.append("")

            row.append("%s" % instanceId)
            row.append("%s" % instanceDetails['CacheNodeType'])
            row.append("%s" % instanceDetails['PreferredAvailabilityZone'])
            for metric in getMetrics():
                row.append(getMetric(cloudWatch, instanceId, node.get('CacheNodeId'), metric, options))
            for metric in getCmdMetrics():
                row.append(getCmdMetric(cloudWatch, instanceId, node.get('CacheNodeId'), metric, options))
            df.loc[i] = row
            row = []
            i += 1

    df.sort_values(by=['NodeId'])


def getReservedInstances(clustersInfo):
    #create the dataframe
    dfColumns = ["Instance Type","Count","Remaining Time (days)"]
    df = pd.DataFrame(columns=dfColumns)

    row = []
    i = 0

    for instanceId, instanceDetails in clustersInfo['elc_reserved_instances'].items():
        row.append(("%s" % instanceId))
        row.append(("%s" % instanceDetails['count']))
        row.append(("%s," % instanceDetails['expiry_time']))
        df.loc[i] = row
        i = i + 1
        row = []
    
    return(df, i)

def getCosts(session):
    #query pricing
    pr = session.client('ce')
    now = datetime.datetime.now()
    start = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    pricingData = pr.get_cost_and_usage(TimePeriod={'Start': start, 'End':  end}, 
        Granularity='MONTHLY',
        Filter={"And": [{"Dimensions": {'Key': 'REGION', 'Values': [session.region_name]}},
        {"Dimensions": {'Key': 'SERVICE', 'Values':['Amazon ElastiCache']}}]},
        Metrics=['UnblendedCost'])

    costs = 0
    for res in pricingData['ResultsByTime']:
        costs = costs + float(res['Total']['UnblendedCost']['Amount'])

    return(costs)


def processAWSAccount(config, section, options):
    print('Grab a coffee this script takes a while...')
    # connect to ElastiCache 
    # aws key, secret and region
    region = config.get(section, 'region')
    accessKey = config.get(section, 'aws_access_key_id')
    secretKey = config.get(section, 'aws_secret_access_key')

    session = boto3.Session(
        aws_access_key_id=accessKey, 
        aws_secret_access_key=secretKey,
        region_name=region)

    outputFilePath = "%s/%s-%s.xlsx" % (options.outDir, section, region)
    writer = pd.ExcelWriter(outputFilePath, engine='xlsxwriter')

    print('Writing Headers')
    clusterDF = createDataFrame()

    print('Gathering data...')
    clustersInfo = getClustersInfo(session)

    print('Writing data...')
    writeClusterInfo(clusterDF, clustersInfo, session, options)
    clusterDF.to_excel(writer, 'ClusterData')

    (reservedDF, numOfInstances) = getReservedInstances(clustersInfo)
    reservedDF.to_excel(writer, 'ReservedData')
    ws = writer.sheets['ReservedData']
    costs = getCosts(session)
    ws.write(numOfInstances + 2, 0, "####Total costs per month####  %s" % costs)    

    writer.save()

def main():
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="configFile",
                  help="Location of configuration file", metavar="FILE")
    parser.add_option("-d", "--out-dir", dest="outDir", default=".",
                  help="directory to write the results in", metavar="PATH")
    parser.add_option("-p", "--days", dest="statsDays", default="7",
                  help="day from which to fetch data", metavar="DATE")
    
    (options, args) = parser.parse_args()
    if options.configFile == None:
        print("Please run with -h for help")
        sys.exit(1)

    config = ConfigParser()
    config.read(options.configFile)

    for section in config.sections():
        if not os.path.isdir(options.outDir):
            os.makedirs(options.outDir)

        processAWSAccount(config, section, options)


if __name__ == "__main__" :
    main()
