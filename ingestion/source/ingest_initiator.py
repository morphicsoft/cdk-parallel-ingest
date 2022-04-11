import json
import os
from dataclasses import dataclass

import boto3

from aws_lambda_powertools import Logger, Tracer

log = Logger()
tracer = Tracer()

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


@dataclass
class S3Event:
    bucket: str
    key: str

    @property
    def url(self):
        return f"s3://{self.bucket}/{self.key}"


def s3_events(payload):
    for record in payload["Records"]:
        yield S3Event(
            bucket=record["s3"]["bucket"]["name"], key=record["s3"]["object"]["key"]
        )


def sfn_client():
    return boto3.client('stepfunctions')


@tracer.capture_lambda_handler
def handler(event, _):
    log.info(f"initiator.setup.handler received {event}")

    for s3_event in s3_events(event):
        log.info(f"Starting state machine {STATE_MACHINE_ARN} for {s3_event.url}")
        response = sfn_client().start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({"bucket": s3_event.bucket, "key": s3_event.key})
        )
        log.info(f"start_execution response: {response}")
