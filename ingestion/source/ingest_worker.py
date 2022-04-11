import csv
import json
import os
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger, Tracer

from smart_open import open

log = Logger()
tracer = Tracer()

TARGET_TABLE_NAME = os.environ["TARGET_TABLE_NAME"]

CSV_HEADINGS = (
    "udprn",
    "uprn",
    "confidence",
    "confidenceBand",
    "estimateValue",
    "estimateRangeLower",
    "estimateRangeUpper"
)


def dynamo_table():
    d = boto3.resource("dynamodb")
    return d.Table(TARGET_TABLE_NAME)


def sfn_client():
    return boto3.client("stepfunctions")


def sfn_callback(client, token):
    log.info(f"Calling send_task_success with token {token}")
    response = client.send_task_success(
        taskToken=token,
        output=json.dumps({"status": True}),  # send _any_ output for now
    )
    log.info(f"send_task_success response: {response}")


@tracer.capture_method
def s3_to_dynamo(bucket, key, start, end):
    table = dynamo_table()
    current_time = str(datetime.now())

    with open(f"s3://{bucket}/{key}") as s3_file:
        reader = csv.DictReader(s3_file, fieldnames=CSV_HEADINGS)
        count = 0
        log.info(f"Seeking to position {start}")
        while count < start:
            _ = next(s3_file)
            count += 1
        log.info(f"Starting reading from {count} to {end}")
        with table.batch_writer(overwrite_by_pkeys=["uprn"]) as batch:
            while count < end:
                row = next(reader)
                count += 1
                batch.put_item(
                    Item={
                        "uprn": row["uprn"],
                        "source_file_name": key,
                        "created_at": current_time,
                        "confidence": row["confidence"],
                        "confidence_band": row["confidenceBand"],
                        "estimate_value": row["estimateValue"],
                        "estimate_range_lower": row["estimateRangeLower"],
                        "estimate_range_upper": row["estimateRangeUpper"],
                    }
                )
            if count % 1000 == 0:
                log.info(f"Processed {count} rows.")
        log.info(f"Finished reading file, processed {count} total rows.")


@tracer.capture_lambda_handler
def handler(event, _):
    log.info(f"ingestion.parallel_file.handler received {event}")
    payload = event["payload"]
    callback_token = event["token"]
    s3_to_dynamo(payload['bucket'], payload['key'], payload["start"], payload["end"])
    sfn_callback(sfn_client(), callback_token)
