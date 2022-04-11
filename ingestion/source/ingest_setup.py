from dataclasses import dataclass

from aws_lambda_powertools import Logger, Tracer
from smart_open import open

log = Logger()
tracer = Tracer()

BATCH_SIZE = 50000  # number of lines


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


@tracer.capture_method
def s3_count(s3_event):
    log.info(f"Counting lines in file {s3_event.url}")
    with open(s3_event.url) as source_file:
        count = sum(1 for _ in source_file)

    log.info(f"File contains {count} lines.")
    return count


def batch_ranges(count, batch_size):
    for x in range(0, count, batch_size):
        start = x
        end = x + batch_size - 1 if x + batch_size <= count else count - 1
        yield start, end


@tracer.capture_lambda_handler
def handler(event, _):
    log.info(f"ingestion.setup.handler received {event}")

    s3_event = S3Event(event["bucket"], event["key"])
    line_count = s3_count(s3_event)

    result = [
        {"bucket": s3_event.bucket, "key": s3_event.key, "start": start, "end": end}
        for start, end in batch_ranges(line_count, BATCH_SIZE)
    ]

    log.info(f"Returning {result}")

    return result
