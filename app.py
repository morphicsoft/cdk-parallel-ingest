#!/usr/bin/env python3
import os

import aws_cdk as cdk

from ingestion.stack.ingestion_stack import ParallelIngestionStack

log_level = os.getenv("LOG_LEVEL") or "INFO"

app = cdk.App()

ingestion_stack = ParallelIngestionStack(app, "ingestion-stack", log_level)

app.synth()
