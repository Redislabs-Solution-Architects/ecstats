# -*- coding: utf-8 -*-

import argparse
import concurrent.futures
import os
import time
from pathlib import Path

import pandas as pd
import redis

debug_flag = False


# Print debug messages
def debug(msg):
    if debug_flag:
        print(msg)


def get_value(value):
    if ',' not in value or '=' not in value:
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            return value
    else:
        sub_dict = {}
        for item in value.split(','):
            k, v = item.rsplit('=', 1)
            sub_dict[k] = get_value(v)
        return sub_dict


def native_str(x):
    return x if isinstance(x, str) else x.decode('utf-8', 'replace')


def parse_response(response):
    """
        Parse the result of Redis's INFO command into a Python dict
        Args:
            response: the response from the info command
        Returns:
            command stats output
    """
    res = {}
    response = native_str(response)

    for line in response.splitlines():
        if line and not line.startswith('#'):
            if line.find(':') != -1:
                # Split, the info fields keys and values.
                # Note that the value may contain ':'. but the 'host:'
                # pseudo-command is the only case where the key contains ':'
                key, value = line.split(':', 1)
                if key == 'cmdstat_host':
                    key, value = line.rsplit(':', 1)
                res[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                res.setdefault('__raw__', []).append(line)

    return res


def get_cmd_metrics():
    metrics = [
        'HashBasedCmds', 'HyperLogLogBasedCmds', 'KeyBasedCmds', 'ListBasedCmds', 'SetBasedCmds',
        'SortedSetBasedCmds', 'StringBasedCmds', 'StreamBasedCmds', 'TotalOps']
    return metrics


def get_metrics():
    metrics = [
        'CurrItems', 'BytesUsedForCache', 'CurrConnections', 'HA?', 'Limited HA?', 'Cluster API?', 'Memory Limit (GB)',
        'Ops/Sec']
    return metrics


def create_data_frame():
    """Create an empty dataframe with headers
    Args:
    Returns:
    The newely created pandas dataframe
    """
    df_columns = ["DB Name", "Node Type"]
    for metric in get_metrics():
        df_columns.append(('%s' % metric))
    for metric in get_cmd_metrics():
        df_columns.append(('%s' % metric))
        # dfColumns.append(('%s (peak over %s minutes)' % (metric, duration)))
    df = pd.DataFrame(columns=df_columns)
    return df


def get_command_by_args(cmds1, cmds2, *args):
    count = 0
    for cmd in args:
        command = 'cmdstat_%s' % cmd
        if command in cmds1:
            count += cmds2[command]['calls'] - cmds1[command]['calls']
    return count


def process_node(row, node, is_master_shard, duration):
    """
        Get the current command stats of the passed node
        Args:
            row: a row from the input file
            node: the node to be processed
            is_master_shard: is master shard
            duration: the duration between runs
        Returns:
            command stats output
    """
    params = node.split(':')
    if pd.isnull(row['Password']):
        client = redis.Redis(host=params[0], port=params[1])
    else:
        if pd.isnull(row['User (ACL)']):
            client = redis.Redis(host=params[0], port=params[1], password=row['Password'])
        else:
            client = redis.Redis(host=params[0], port=params[1], password=row['Password'], username=row['User (ACL)'])

    print('Processing %s' % row['Redis Host'])
    result = {}

    # first run
    res1 = parse_response(client.execute_command('info commandstats'))
    info1 = client.execute_command('info')
    time.sleep(duration * 60)

    # second run
    res2 = parse_response(client.execute_command('info commandstats'))
    info2 = client.execute_command('info')

    result['DB Name'] = row['Redis Host'].replace('.', '-')
    result['BytesUsedForCache'] = info2['used_memory_peak']
    result['Memory Limit (GB)'] = info2['used_memory_peak'] / 1024 ** 3
    result['CurrConnections'] = info2['connected_clients']

    if 'cluster_enabled' in info2 and info2['cluster_enabled'] == 1:
        result['Cluster API?'] = True
    else:
        result['Cluster API?'] = False

    if is_master_shard:
        result['Node Type'] = 'Master'
    else:
        result['Node Type'] = 'Replica'

    if info2['connected_slaves'] > 0:
        result['HA?'] = True
        if info2['connected_slaves'] < 2:
            result['Limited HA?'] = True
        else:
            result['Limited HA?'] = False
    else:
        result['HA?'] = False
        result['Limited HA?'] = True

    result['TotalOps'] = info2['total_commands_processed'] - info1['total_commands_processed']
    result['Ops/Sec'] = result['TotalOps'] / (60 * duration)

    # String
    result['StringBasedCmds'] = get_command_by_args(res1, res2, 'get', 'set', 'incr', 'decr', 'incrby', 'decrby')

    # Hash
    result['HashBasedCmds'] = get_command_by_args(res1, res2, 'hget', 'hset', 'hgetall', 'hmget', 'hsetnx')

    # HyperLogLog
    result['HyperLogLogBasedCmds'] = get_command_by_args(res1, res2, 'pfadd', 'pfcount', 'pfmerge')

    # Keys
    result['KeyBasedCmds'] = get_command_by_args(res1, res2, 'del', 'expire', 'unlink')

    # List
    result['ListBasedCmds'] = get_command_by_args(
        res1, res2, 'blpop', 'brpop', 'brpoplpush', 'blmove', 'linsert', 'llen',
        'lpop', 'lpush', 'lpushx', 'lrange', 'lset', 'lrem', 'rpop', 'rpoplpush',
        'rpush', 'rpushx')

    # Sets
    result['SetBasedCmds'] = get_command_by_args(
        res1, res2, 'sadd', 'scard', 'sdiff', 'sdiffstore', 'sinter',
        'sinterstore', 'sismember', 'smismember', 'smembers', 'smove', 'spop',
        'srandmember', 'srem', 'sunion', 'sunionstore', 'sscan')

    # SortedSets
    result['SortedSetBasedCmds'] = get_command_by_args(
        res1, res2, 'bzpopmin', 'bzpopmax', 'zadd', 'zcard', 'zcount',
        'zdiff', 'zdiffstore', 'zincrby', 'zinter', 'zinterstore',
        'zlexcount', 'zpopmax', 'zpopmin', 'zrange', 'zrangebylex',
        'zrevrangebylex', 'zrangebyscore', 'zrank', 'zrem',
        'zremrangebylex', 'zremrangebyrank', 'zremrangebyscore',
        'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore', 'zunion',
        'zmscore', 'zunionstore', 'zscan')

    # Streams
    result['StreamBasedCmds'] = get_command_by_args(res1, res2, 'xadd', 'xtrim', 'xdel', 'xrange', 'xrevrange', 'xlen',
                                                 'xread', 'xgroup', 'xreadgroup', 'xack', 'xclaim', 'xpending')
    result['CurrItems'] = 0
    for x in range(10):
        db = 'db%s' % x
        if db in info2:
            debug('num of keys %s' %info2[db]['keys'])
            result['CurrItems'] += info2[db]['keys']
    result_arr = [result]
    return result_arr


def process_db(row, output_df, duration):
    """
        Get the current command stats of the DB
        Args:
            row: a row from the input file
            output_df: the output data frame
            duration: the duration between runs
        Returns:
            command stats output
    """
    if pd.isnull(row['Password']):
        client = redis.Redis(host=row['Redis Host'], port=row['Port'], socket_timeout=10)
    else:
        if pd.isnull(row['User (ACL)']):
            client = redis.Redis(host=row['Redis Host'], port=row['Port'], password=row['Password'], socket_timeout=10)
        else:
            client = redis.Redis(host=row['Redis Host'], port=row['Port'], password=row['Password'],
                                 username=row['User (ACL)'], socket_timeout=10)

    try:
        client.ping()
    except:
        print('Error connecting to Redis %s' % row['Redis Host'])
        return output_df

    info = client.execute_command('info')
    is_clustered = False
    if 'cluster_enabled' in info and info['cluster_enabled'] == 1:
        is_clustered = True

    if is_clustered is True:
        nodes = client.execute_command('cluster nodes')
    else:
        nodes = {'%s:%s' % (row['Redis Host'], row['Port']): {'flags': 'master', 'connected': True}}

    with concurrent.futures.ProcessPoolExecutor():
        for node, stats in nodes.items():
            is_master_shard = False
            if stats['flags'].find('master') >= 0:
                is_master_shard = True

            if stats['connected'] is True:
                output_df = output_df.append(process_node(row, node, is_master_shard, duration), ignore_index=True)
    return output_df


def my_agg(x):
    names = {
        'Amount mean': x['Amount'].mean(),
        'Amount std': x['Amount'].std(),
        'Amount range': x['Amount'].max() - x['Amount'].min(),
        'Score Max': x['Score'].max(),
        'Score Sum': x['Score'].sum(),
        'Amount Score Sum': (x['Amount'] * x['Score']).sum()}
    return pd.Series(names, index=['Amount range', 'Amount std', 'Amount mean',
                                   'Score Sum', 'Score Max', 'Amount Score Sum'])


def process_file(input_file_path, output_file_path, duration):
    """
        Process the entire input file
        Args:
            input_file_path: the file path to be processed
            output_file_path: the file path for the processed file
            duration: duration between each run
        Returns:
            None
    """
    input_df = pd.read_excel(input_file_path, header=0, sheet_name="Redis Sizing Input")
    output_df = create_data_frame()

    with concurrent.futures.ProcessPoolExecutor():
        for (index, row) in input_df.iterrows():
            output_df = process_db(row, output_df, duration)

    output_df = output_df.rename(columns={
        "HashBasedCmds": 'HashBasedCmds (peak over %s minutes)' % duration,
        'HyperLogLogBasedCmds': 'HyperLogLogBasedCmds (peak over %s minutes)' % duration,
        'KeyBasedCmds': 'KeyBasedCmds (peak over %s minutes)' % duration,
        'ListBasedCmds': 'ListBasedCmds (peak over %s minutes)' % duration,
        'SetBasedCmds': 'SetBasedCmds (peak over %s minutes)' % duration,
        'SortedSetBasedCmds': 'SortedSetBasedCmds (peak over %s minutes)' % duration,
        'StringBasedCmds': 'StringBasedCmds (peak over %s minutes)' % duration,
        'TotalOps': 'TotalOps (peak over %s minutes)' % duration
    })

    grouped = output_df.groupby('DB Name').aggregate({
        'DB Name': 'last',
        'HashBasedCmds (peak over %s minutes)' % duration: 'sum',
        'HyperLogLogBasedCmds (peak over %s minutes)' % duration: 'sum',
        'KeyBasedCmds (peak over %s minutes)' % duration: 'sum',
        'ListBasedCmds (peak over %s minutes)' % duration: 'sum',
        'SetBasedCmds (peak over %s minutes)' % duration: 'sum',
        'SortedSetBasedCmds (peak over %s minutes)' % duration: 'sum',
        'StringBasedCmds (peak over %s minutes)' % duration: 'sum',
        'TotalOps (peak over %s minutes)' % duration: 'sum',
        'CurrItems': 'sum',
        'BytesUsedForCache': 'sum',
        'CurrConnections': 'sum',
        'HA?': 'last',
        'Limited HA?': 'last',
        'Cluster API?': 'last',
        'Memory Limit (GB)': 'last',
        'Ops/Sec': 'sum'
    })

    with (pd.ExcelWriter(output_file_path, engine='xlsxwriter')) as writer:
        output_df.to_excel(writer, 'Raw Input Data')
        grouped.to_excel(writer, 'Redis Sizing Input')


def main():
    global debug_flag

    parser = argparse.ArgumentParser()
    parser.add_argument("inputFile",
                        help='''
    The Excel file containing Redis endpoints to pull stats from
                        ''')
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        help="Number of minutes between gathering information from the endpoint (default 5)",
        default=1)

    parser.add_argument(
        "-o",
        "--output-file",
        help='''
    Name of file results are written to. Defaults to input file name with
     '-out' appended to stem.
    ''')
    args = parser.parse_args()

    # Startup parameters
    input_file = args.inputFile

    if args.output_file is None:
        output_dir = os.path.dirname(input_file)
        if output_dir == "":
            output_dir = "."

        output_file = "%s/%s-out.xlsx" % (output_dir, Path(input_file).stem)
    else:
        output_file = args.output_file

    print("outputFile will be: {}".format(output_file))
    process_file(input_file, output_file, args.duration)


if __name__ == "__main__":
    main()
