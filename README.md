# cdk-parallel-ingest

CDK-based solution which handles ingest of large files on S3.


## Getting started

### Pre-requisites

Python 3.n

### Setup

Python Invoke is used for most tasks.

To get started, create a virtualenv, activate it, then...

```bash
pip install -r bootstrap-requirements.txt
inv update
inv sync
inv package
cdk deploy
```
