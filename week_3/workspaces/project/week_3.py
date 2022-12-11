from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    String,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"}
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    # get the stock with the highest number of stock.high
    highest_stock = max(stocks, key=lambda stock:stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@op (
    required_resource_keys={"redis"}
)
def put_redis_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(stock_aggregation.date), value=str(stock_aggregation.high))


@op (
    required_resource_keys={"s3"}
)
def put_s3_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key=stock_aggregation.date, data=stock_aggregation)


@graph
def week_3_pipeline():
    high_stocks = process_data(get_s3_data())
    put_redis_data(high_stocks)
    put_s3_data(high_stocks)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(
    partition_keys=[str(i) for i in range(1, 11)]
)
def docker_config(partition_key: String):
    docker_config = docker.copy()
    docker_config['ops'] = {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}
    return docker_config


week_3_pipeline_local = week_3_pipeline.to_job(
    name = "week_3_pipeline_local",
    config = local,
    resource_defs = {
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource()
    }
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name = "week_3_pipeline_docker",
    resource_defs = {
        "s3": s3_resource,
        "redis": redis_resource
    },
    op_retry_policy = RetryPolicy(max_retries=10, delay=1)
)


week_3_schedule_local = ScheduleDefinition(
    job = week_3_pipeline_local, 
    cron_schedule = "*/15 * * * *"
)


@schedule(
    job=week_3_pipeline_docker,
    cron_schedule="0 * * * *"
)
def week_3_schedule_docker():
    return RunRequest(
        run_config=docker_config       
    )


@sensor(
    job=week_3_pipeline_docker,
    minimum_interval_seconds=30
)
def week_3_sensor_docker(context):
    s3_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://localstack:4566"
    )
    if not s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
    else:
        for s3_key in s3_keys:
            docker_config = docker.copy()
            docker_config['ops'] = {"get_s3_data": {"config": {"s3_key": f"{s3_key}"}}}
            yield RunRequest(
                run_key=s3_key, 
                run_config=docker_config
            )
