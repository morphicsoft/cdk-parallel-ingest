import aws_cdk as cdk

XRAY_TRACING = cdk.aws_lambda.Tracing.DISABLED
REMOVAL_POLICY = cdk.RemovalPolicy.DESTROY
MAX_CONCURRENCY = 100


class ParallelIngestionStack(cdk.Stack):
    def __init__(self, scope: cdk.App, id, log_level, **kwargs):
        super().__init__(scope, id, **kwargs)

        source_bucket = cdk.aws_s3.Bucket(
            self,
            "source-bucket",
            removal_policy=REMOVAL_POLICY
        )

        target_table = cdk.aws_dynamodb.Table(
            self,
            "estimates-table",
            partition_key=cdk.aws_dynamodb.Attribute(name="uprn", type=cdk.aws_dynamodb.AttributeType.STRING),
            sort_key=cdk.aws_dynamodb.Attribute(name="source_file_name", type=cdk.aws_dynamodb.AttributeType.STRING),
            billing_mode=cdk.aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            # stream=cdk.aws_dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=REMOVAL_POLICY
        )

        setup_handler = cdk.aws_lambda.Function(
            self,
            "setup-handler",
            code=cdk.aws_lambda.Code.from_asset("dist/ingestion"),
            handler="ingest_setup.handler",
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_9,
            timeout=cdk.Duration.minutes(15),
            environment={
                "POWERTOOLS_SERVICE_NAME": "setup-handler",
                "LOG_LEVEL": log_level
            },
            tracing=XRAY_TRACING,
        )

        source_bucket.grant_read(setup_handler)

        setup = cdk.aws_stepfunctions_tasks.LambdaInvoke(self, "setup", lambda_function=setup_handler)

        map = cdk.aws_stepfunctions.Map(
            self,
            "map-ingest-workers",
            # max_concurrency=MAX_CONCURRENCY,
            items_path=cdk.aws_stepfunctions.JsonPath.string_at("$.Payload"),
        )

        parallel_ingest_handler = cdk.aws_lambda.Function(
            self,
            "parallel-ingest-handler",
            code=cdk.aws_lambda.Code.from_asset("dist/ingestion"),
            handler="ingest_worker.handler",
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_9,
            timeout=cdk.Duration.minutes(15),
            environment={
                "POWERTOOLS_SERVICE_NAME": "parallel-ingest-handler",
                "LOG_LEVEL": log_level,
                "TARGET_TABLE_NAME": target_table.table_name

            },
            tracing=XRAY_TRACING,
        )

        send_task_policy = cdk.aws_iam.PolicyStatement(
            resources=[f"arn:aws:states:{self.region}:{self.account}:stateMachine:*"],
            actions=[
                "states:SendTaskFailure",
                "states:SendTaskHeartbeat",
                "states:SendTaskSuccess",
            ],
        )

        # TODO: should be able to use `state_machine.grant_task_response(parallel_file_handler)`,
        #  but that gives circular dependency
        parallel_ingest_handler.add_to_role_policy(send_task_policy)
        source_bucket.grant_read(parallel_ingest_handler)
        target_table.grant_read_write_data(parallel_ingest_handler)

        parallel_process_file = cdk.aws_stepfunctions_tasks.LambdaInvoke(
            self,
            "parallel-ingest-file",
            lambda_function=parallel_ingest_handler,
            integration_pattern=cdk.aws_stepfunctions.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=cdk.aws_stepfunctions.TaskInput.from_object({
                "payload": cdk.aws_stepfunctions.JsonPath.string_at("$"),
                "token": cdk.aws_stepfunctions.JsonPath.task_token,
            }),
            timeout=cdk.Duration.minutes(15)
        )

        map.iterator(parallel_process_file)

        definition = setup.next(map)

        state_machine = cdk.aws_stepfunctions.StateMachine(
            self,
            "ingest-state-machine",
            definition=definition,
            timeout=cdk.Duration.minutes(120),
        )

        # state_machine.grant_task_response(parallel_file_handler)

        startup_handler = cdk.aws_lambda.Function(
            self,
            "initiator",
            runtime=cdk.aws_lambda.Runtime.PYTHON_3_9,
            code=cdk.aws_lambda.Code.from_asset("dist/ingestion"),
            handler="ingest_initiator.handler",
            timeout=cdk.Duration.seconds(10),
            environment={
                "POWERTOOLS_SERVICE_NAME": "ingestion.initiator",
                "LOG_LEVEL": log_level,
                "STATE_MACHINE_ARN": state_machine.state_machine_arn
            },
            tracing=XRAY_TRACING,
        )

        state_machine.grant_start_execution(startup_handler)
        startup_handler.add_event_source(
            cdk.aws_lambda_event_sources.S3EventSource(
                source_bucket, events=[cdk.aws_s3.EventType.OBJECT_CREATED]
            )
        )

