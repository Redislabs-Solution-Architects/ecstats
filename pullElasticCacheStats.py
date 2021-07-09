# -*- coding: utf-8 -*-

# input params
# path to the config file, see pullStatsConfig.json

from optparse import OptionParser

import boto3
import datetime
import os
import pandas as pd
import sys

# Metric Collection Period (in days)
METRIC_COLLECTION_PERIOD_DAYS = 7

# Aggregation Duration (in seconds)
AGGREGATION_DURATION_SECONDS = 3600

# Different versions of python 2.7 have renamed modules
try:
    from configparser import ConfigParser

except ImportError:
    from ConfigParser import ConfigParser


def get_cmd_metrics():
    metrics = [
        'GetTypeCmds',
        'HashBasedCmds',
        'HyperLogLogBasedCmds',
        'KeyBasedCmds',
        'ListBasedCmds',
        'SetBasedCmds',
        'SetTypeCmds',
        'SortedSetBasedCmds',
        'StringBasedCmds',
        'StreamBasedCmds']
    return metrics


def get_metrics():
    metrics = [
        'CurrItems',
        'BytesUsedForCache',
        'CacheHits',
        'CacheMisses',
        'CurrConnections',
        'NetworkBytesIn',
        'NetworkBytesOut',
        'NetworkPacketsIn',
        'NetworkPacketsOut',
        'EngineCPUUtilization',
        'Evictions',
        'ReplicationBytes',
        'ReplicationLag']
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


def get_clusters_info(session):
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
            if (instance['CacheClusterStatus'] == 'available' and
               instance['Engine'] == 'redis'):
                cluster_id = instance['CacheClusterId']
                results['elc_running_instances'][cluster_id] = instance

    paginator = conn.get_paginator('describe_reserved_cache_nodes')
    page_iterator = paginator.paginate()

    # Loop through active ElastiCache RIs and record their type and engine.
    for page in page_iterator:
        for reserved_instance in page['ReservedCacheNodes']:
            if (reserved_instance['State'] == 'active' and
               reserved_instance['ProductDescription'] == 'redis'):
                instance_type = reserved_instance['CacheNodeType']
                # No end datetime is returned, so calculate from 'StartTime'
                # (a `DateTime`) and 'Duration' in seconds (integer)
                expiry_time = reserved_instance[
                    'StartTime'] + datetime.timedelta(
                    seconds=reserved_instance['Duration'])
                results['elc_reserved_instances'][instance_type] = {
                    'count': reserved_instance['CacheNodeCount'],
                    'expiry_time': calc_expiry_time(expiry=expiry_time)
                }

    return results


def get_metric(cloud_watch, cluster_id, node, metric, options):
    """Write node related metrics to file
    Args:
        ClusterId, node and metric to write
    Returns:
    The metric value
    """
    today = datetime.date.today() + datetime.timedelta(days=1)
    then = today - datetime.timedelta(days=METRIC_COLLECTION_PERIOD_DAYS)
    response = cloud_watch.get_metric_statistics(
        Namespace='AWS/ElastiCache',
        MetricName=metric,
        Dimensions=[
            {'Name': 'CacheClusterId', 'Value': cluster_id},
            {'Name': 'CacheNodeId', 'Value': node}
        ],
        StartTime=then.isoformat(),
        EndTime=today.isoformat(),
        Period=AGGREGATION_DURATION_SECONDS,
        Statistics=['Sum']
    )

    rec_max = 0
    for rec in response['Datapoints']:
        if rec['Sum'] > rec_max:
            rec_max = rec['Sum']

    return rec_max


def create_data_frame():
    """Create an empty dataframe with headers
    Args:
    Returns:
    The newely created pandas dataframe
    """
    df_columns = ["ClusterId", "NodeId", "NodeType", "Region"]
    for metric in get_metrics():
        df_columns.append(('%s (max over last week)' % metric))
    for metric in get_cmd_metrics():
        df_columns.append(('%s (peak last week / hour)' % metric))
    df = pd.DataFrame(columns=df_columns)
    return df


def write_cluster_info(df, clusters_info, session, options):
    """Write all the data gathered to the file
    Args:
        The cluster information dictionary
    Returns:
    """
    cloud_watch = session.client('cloudwatch')
    row = []
    i = 0

    running_instances = clusters_info['elc_running_instances']
    for instanceId, instanceDetails in running_instances.items():
        for node in instanceDetails.get('CacheNodes'):
            print(
                "Getting node % s details" %
                (instanceDetails['CacheClusterId']))
            if 'ReplicationGroupId' in instanceDetails:
                row.append("%s" % instanceDetails['ReplicationGroupId'])
            else:
                row.append("")

            row.append("%s" % instanceId)
            row.append("%s" % instanceDetails['CacheNodeType'])
            row.append("%s" % instanceDetails['PreferredAvailabilityZone'])
            for metric in get_metrics():
                row.append(
                    get_metric(
                        cloud_watch,
                        instanceId,
                        node.get('CacheNodeId'),
                        metric,
                        options))
            for metric in get_cmd_metrics():
                row.append(
                    get_metric(
                        cloud_watch,
                        instanceId,
                        node.get('CacheNodeId'),
                        metric,
                        options))
            df.loc[i] = row
            row = []
            i += 1

    df.sort_values(by=['NodeId'])


def get_reserved_instances(clusters_info):
    # create the dataframe
    df_columns = ["Instance Type", "Count", "Remaining Time (days)"]
    df = pd.DataFrame(columns=df_columns)

    row = []
    i = 0
    reserved_instances = clusters_info['elc_reserved_instances']
    for instanceId, instanceDetails in reserved_instances.items():
        row.append(("%s" % instanceId))
        row.append(("%s" % instanceDetails['count']))
        row.append(("%s," % instanceDetails['expiry_time']))
        df.loc[i] = row
        i = i + 1
        row = []

    return (df, i)


def get_costs(session):
    # query pricing
    pr = session.client('ce')
    now = datetime.datetime.now()
    start = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    pricing_data = pr.get_cost_and_usage(TimePeriod={'Start': start,
                                                     'End': end},
                                         Granularity='MONTHLY',
                                         Filter={
        "And": [{"Dimensions": {'Key': 'REGION',
                                'Values': [session.region_name]}},
                {"Dimensions": {'Key': 'SERVICE',
                                'Values': ['Amazon ElastiCache']}}]},
        Metrics=['UnblendedCost'])

    costs = 0
    for res in pricing_data['ResultsByTime']:
        costs = costs + float(res['Total']['UnblendedCost']['Amount'])

    return costs


def process_aws_account(config, section, options):
    print('Grab a coffee this script takes a while...')
    # connect to ElastiCache
    # aws key, secret and region
    region = config.get(section, 'region')
    access_key = config.get(section, 'aws_access_key_id')
    secret_key = config.get(section, 'aws_secret_access_key')

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region)

    output_file_path = "%s/%s-%s.xlsx" % (options.outDir, section, region)
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')

    print('Writing Headers')
    cluster_df = create_data_frame()

    print('Gathering data...')
    clusters_info = get_clusters_info(session)

    print('Writing data...')
    write_cluster_info(cluster_df, clusters_info, session, options)
    cluster_df.to_excel(writer, 'ClusterData')

    (reservedDF, numOfInstances) = get_reserved_instances(clusters_info)
    reservedDF.to_excel(writer, 'ReservedData')
    ws = writer.sheets['ReservedData']
    costs = get_costs(session)
    ws.write(
        numOfInstances +
        2,
        0,
        "####Total costs per month####  %s" %
        costs)

    writer.save()


def main():
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="configFile",
                      help="Location of configuration file", metavar="FILE")
    parser.add_option("-d", "--out-dir", dest="outDir", default=".",
                      help="directory to write the results in", metavar="PATH")

    (options, args) = parser.parse_args()
    if options.configFile is None:
        print("Please run with -h for help")
        sys.exit(1)

    config = ConfigParser()
    config.read(options.configFile)

    for section in config.sections():
        if not os.path.isdir(options.outDir):
            os.makedirs(options.outDir)

        process_aws_account(config, section, options)


if __name__ == "__main__":
    main()
