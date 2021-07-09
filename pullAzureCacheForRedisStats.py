from azure.identity import DefaultAzureCredential
from azure.mgmt.redis import RedisManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.subscription import SubscriptionClient
import datetime
import pandas as pd
from pathlib import Path
import argparse

# The measurement collection period in days.
METRIC_COLLECTION_PERIOD_DAYS = 7

# The aggregation period
# (see https://en.wikipedia.org/wiki/ISO_8601#Durations )
AGGREGATION_PERIOD = "PT1H"

# Seconds in the aggregation period
SECONDS_PER_AGGREGATION_PERIOD = 3600

# The Azure metrics to be collected for each cluster
METRICS = "totalcommandsprocessed,usedmemory"


def get_metrics(mc, resource_id):
    today = datetime.date.today() + datetime.timedelta(days=1)
    then = today - datetime.timedelta(days=METRIC_COLLECTION_PERIOD_DAYS)
    timespan = "{}/{}".format(then, today)
    metrics_data = mc.metrics.list(
        resource_id,
        metricnames=METRICS,
        timespan=timespan,
        interval=AGGREGATION_PERIOD,
        aggregation="Total")

    # WARNING - if you change the aggregation above then you MUST change the
    # accessor below to its lower case equivalent
    # e.g. if you use "Maximum" above then you'd use metric_value.maximum
    # below.

    # Ugh - this is a very painful expression caused by the nesting of various
    # arrays within the Azure data types.
    return [
        max(
            [
                metric_value.total
                for ts in metric.timeseries
                for metric_value in ts.data
                if metric_value.total is not None
            ]
        )
        for metric in metrics_data.value
    ]


def get_resource_group(cluster):
    return cluster.id.split("/")[4]


def process_cluster(cluster, mc):
    non_metrics = [
        get_resource_group(cluster),
        cluster.name,
        cluster.sku.name,
        cluster.replicas_per_master,
        cluster.shard_count
    ]
    metrics = get_metrics(mc, cluster.id)
    return non_metrics + metrics


def get_subscription_info(credential):
    return [[sub.subscription_id, MonitorManagementClient(
            credential=credential,
            subscription_id=sub.subscription_id
            )]
            for sub in
            SubscriptionClient(credential=credential).subscriptions.list()]


def list_clusters(credential, subscription_id):
    return RedisManagementClient(credential, subscription_id).redis.list()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--out-dir", dest="outDir", default=".",
                        help="directory to write the results in",
                        metavar="PATH")
    args = parser.parse_args()
    output_file_path = Path(args.outDir) / "AzureStats.xlsx"

    azure_credential = DefaultAzureCredential()
    metrics = [[sub_info[0]] + process_cluster(cluster, sub_info[1])
               for sub_info in get_subscription_info(azure_credential)
               for cluster in list_clusters(azure_credential, sub_info[0])]
    df = pd.DataFrame(metrics, columns=["Subscription ID",
                                        "Resource Group",
                                        "DB Name",
                                        "SKU",
                                        "Replicas per Master",
                                        "Shard Count",
                                        "Total Commands Processed",
                                        "Used Memory"])

    with (pd.ExcelWriter(output_file_path, engine='xlsxwriter')) as writer:
        df.to_excel(writer, 'ClusterData', index=False)

    print("Results are in {}".format(output_file_path))


if __name__ == "__main__":
    main()
