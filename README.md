# Serverless-Multi-Tenant-Analytics-Platform-on-AWS

A multi-tenant serverless analytics platform built using AWS CDK (Python),
with isolated Lambda functions per application (Campus, Teleo, Pillars, EventGarde),
and a centralized ETL pipeline under Integreat that transforms data from NeonDB
into tenant-specific S3 buckets for use in Power BI.

---

## 📑 Table of Contents

* [Project Structure](#project-structure)
* [Installation](#installation)
* [Usage](#usage)
* [Deployment](#deployment)
* [Testing](#testing)
* [Development Notes](#development-notes)

---

## 📁 Project Structure

```bash
INTEGREAT-ANALYTICS/
├── app.py                     # CDK entrypoint to deploy all stacks
├── cdk.json                   # CDK context configuration file
├── requirements.txt           # CDK runtime dependencies
├── requirements-dev.txt       # Dev/test/lint dependencies
├── source.bat                 # Windows helper script for venv activation + install
├── README.md                  # This file

├── integreat_analytics/       # CDK stack definitions
│   ├── __init__.py
│   ├── tenant_lambda_stack.py # Defines per-tenant Lambda functions
│   └── eventbridge_stack.py   # Defines shared EventBridge scheduler

├── scripts/                   # Python code executed by each Lambda
│   ├── campus/                # Campus tenant Lambda
│   ├── teleo/                 # Teleo tenant Lambda
│   ├── pillars/               # Pillars tenant Lambda
│   ├── evntgarde/             # EventGarde tenant Lambda
│   └── integreat/             # Shared DW ETL pipeline: etl, marts, csv upload

├── tests/                     # Unit tests for CDK stacks or Python logic
│   └── unit/                  # Sample and infra-related tests
│       └── __init__.py
```

---

## ⚙️ Installation

Follow these steps to install and prepare the environment.

### 1. Clone the Repository

```bash
git clone https://github.com/InteGreat-Team/InteGreat-Analytics.git
cd integreat-analytics
```

### 2. Create and Activate a Virtual Environment

```bash
# Create venv (if not yet created)
python -m venv .venv

# Activate it
# On Windows:
source.bat

# On macOS/Linux:
source .venv/bin/activate
```

### 3. Install Required Packages

```bash
# Runtime requirements
py -m pip install -r requirements.txt

# Development/test requirements
py -m pip install -r requirements-dev.txt
```

> **Note:** Using `py -m pip` is recommended to avoid `pip: command not found` errors on Windows.

---

## 🚀 Usage

### Synth the CDK App

```bash
cdk synth
```

### Deploy All Stacks

```bash
cdk deploy --all
```

This will deploy:

* A Lambda for each tenant under `scripts/<tenant>`
* A single EventBridge rule that triggers all of them at 12MN (GMT+8)
* The shared Integreat DW ETL Lambda

---

## 🧪 Testing

Run tests using:

```bash
pytest
```

Optional test config lives in `pytest.ini`.

---

## 🛠 Development Notes

* Each tenant Lambda handles its own data export logic.
* Integreat handles:

  * Extracting from NeonDB OLTP
  * Transforming into OLAP fact/dim tables
  * Creating materialized views (data marts)
  * Uploading per-tenant CSVs to S3
* All context values (e.g., bucket names) are passed via `cdk.context.json`
* No secrets manager is used; no JWTs are required for backend Lambdas.
* S3 access and Cognito roles are provisioned separately in a Node.js CDK stack.

---

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
