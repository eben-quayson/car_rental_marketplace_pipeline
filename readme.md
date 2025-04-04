Here’s a structured **`README.md`** for your AWS Step Functions and EMR-based data pipeline:  

---

# **AWS Step Functions EMR Pipeline**  

## **Overview**  
This project automates an **ETL pipeline** using **AWS Step Functions** to orchestrate **Amazon EMR**, **Apache Spark**, and **AWS Glue**. The pipeline processes vehicle, location, user, and transaction KPIs, stores the results, and triggers a Glue Crawler for cataloging.  

---

## **Architecture**  

### **Workflow**  
1. **Start EMR Cluster** – Launches an EMR cluster with Spark, Hive, and Hadoop.  
2. **Run Spark Jobs** – Executes two Spark scripts stored in **S3**:  
   - `compute_location_kpi.py`: Processes vehicle and location KPIs.  
   - `compute_user_transactions_kpi.py`: Computes user and transaction KPIs.  
3. **Trigger AWS Glue Crawler** – Updates the data catalog.  
4. **Terminate EMR Cluster** – Shuts down EMR after execution.  

### **AWS Services Used**  
- **AWS Step Functions** – Orchestration  
- **Amazon EMR** – Managed Hadoop/Spark cluster  
- **Amazon S3** – Storage for scripts and logs  
- **AWS Glue** – Data cataloging  
- **AWS IAM** – Access control  

---

## **Deployment**  

### **1️⃣ Prerequisites**  
Ensure you have:  
- An **S3 bucket** for storing scripts/logs.  
- Proper **IAM roles** (`EMR_EC2_DefaultProfile`, `EMR_EC2_Role`).  
- A **subnet ID** for deploying EMR instances.  

### **2️⃣ Upload Spark Scripts to S3**  
```sh
aws s3 cp compute_location_kpi.py s3://rental-marketplace-gyenyame/scripts/
aws s3 cp compute_user_transactions_kpi.py s3://rental-marketplace-gyenyame/scripts/
```

### **3️⃣ Deploy Step Functions State Machine**  
- Create a new **Step Functions** state machine.  
- Copy-paste the JSON definition (`state_machine.json`).  
- Replace placeholders for **S3 paths**, **IAM roles**, and **Subnet IDs**.  

### **4️⃣ Execute the Pipeline**  
Run the state machine via the AWS console or CLI:  
```sh
aws stepfunctions start-execution --state-machine-arn arn:aws:states:region:account-id:stateMachine:your-state-machine
```

---

## **Monitoring & Logs**  
- **Step Functions Execution History** – Track each state.  
- **EMR Logs** – Stored in `s3://rental-marketplace-gyenyame/logs/`.  
- **Spark Job Logs** – View logs from the EMR console or SSH into the cluster.  

---

## **Security Considerations**  
- Use **IAM roles** with the **least privilege**.  
- Enable **S3 encryption** and **logging**.  
- Restrict **EMR security group** access.  

---

## **License**  
This project is licensed under the **MIT License**. See `LICENSE.md` for details.  

---

## **Contributors**  
- **Ebenezer Quayson** – Data Engineer  

---

This **`README.md`** ensures clarity, making it easy to set up, run, and troubleshoot your pipeline! 🚀