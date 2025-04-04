# Architecture Overview

## System Overview
This document describes the architecture of the EMR-based data processing system, which automates Spark job execution and integrates with AWS services.

## Architecture Diagram
[Image](architecture.png)

## Components
### **1. AWS Step Functions**
- Orchestrates the execution of EMR cluster and Spark jobs.
- Handles error recovery and step transitions.

### **2. Amazon EMR**
- Runs Apache Spark for large-scale data processing.
- Configured with `m5.xlarge` instances for **Master** and **Core nodes**.

### **3. AWS S3**
- Stores job scripts (`.py` files).
- Stores job logs and output data.

### **4. AWS Glue**
- Runs a **crawler** to catalog processed data into the Glue Data Catalog.
- Enables structured querying via **AWS Athena**.

### **5. IAM Roles & Policies**
- **EMR Instance Role**: Grants EMR instances access to S3, Glue, and CloudWatch.
- **Step Functions Execution Role**: Allows Step Functions to create EMR clusters and submit jobs.

## Workflow
1. Step Functions **creates an EMR cluster**.
2. EMR **runs Spark scripts** stored in S3.
3. The **Spark job** processes the data and writes results back to S3.
4. AWS Glue **crawls the processed data** and updates the Glue Data Catalog.
5. The EMR cluster **terminates** after job completion.

## Security Considerations
- **Data encryption**: S3 and Glue data are encrypted using AWS KMS.
- **IAM policies**: Only required AWS services and users have access to sensitive resources.
- **Logging**: CloudWatch logs monitor failures and job execution times.

---
