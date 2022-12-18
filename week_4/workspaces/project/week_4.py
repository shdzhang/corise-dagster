from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    group_name="corise"
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]


@asset(
    group_name="corise"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    # get the stock with the highest number of stock.high
    highest_stock = max(stocks, key=lambda stock:stock.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    required_resource_keys={"redis"},
    group_name="corise"
)
def put_redis_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(stock_aggregation.date), value=str(stock_aggregation.high))


@asset(
    required_resource_keys={"s3"},
    group_name="corise"
)
def put_s3_data(context, stock_aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key=stock_aggregation.date, data=stock_aggregation)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={
        "s3": s3_resource, 
        "redis": redis_resource
    },
    resource_config_by_key={
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
        }
    }
)
