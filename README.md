# ETL Data Pipeline Project (Medallion Architecture)

This project implements a modular and scalable ETL pipeline using PySpark and follows the **Medallion Architecture** (Bronze, Silver, Gold). It is designed to run on **Azure Databricks** and reads configuration from external metadata files to orchestrate the flow.

---

## 🗂️ Project Structure

project_root/
│
├── main.py # Main orchestrator script for the ETL pipeline
├── config/
│ └── metadata.json # Metadata configuration used to drive the ETL process
│
├── etl/
│ ├── ingestion/
│ │ └── DataIngestor.py # Contains logic to ingest raw data into Bronze layer
│ │
│ └── transformation/
│ └── DataValidatorAndTransformer.py # Handles validations and transformations to Silver
│
├── utils/
│ ├── logger_config.py # Centralized logger configuration
│ └── etl_pipeline.log # (Optional) log file created during execution

## ⚙️ Features

- 🔁 **Metadata-driven pipeline**: dynamically loads sources, sinks, and transformations from `metadata.json`.
- 🥉 **Bronze Layer**: stores raw ingested data.
- 🥈 **Silver Layer**: stores cleaned and validated data.
- 🥇 **Gold Layer**: optionally generates aggregated views.
- ✅ **Validation**: Invalid rows are logged and saved separately.
- 📄 **Logging**: Console and file logging via a reusable utility.
- 🔧 **Job orchestration ready**: Compatible with Databricks Jobs with task dependencies.

---

## 🚀 How to Run

### 1. Upload to Databricks Workspace (or clone locally)

Clone this repository and open in a Databricks notebook or attach to a cluster if you're using a script-based workflow.

### 2. Prepare Your Metadata

Edit `config/metadata.json` to configure your sources, sinks, and transformation logic.

### 3. Run the Pipeline

Run the `main.py` script to execute the ETL flow:

```bash
python main.py
