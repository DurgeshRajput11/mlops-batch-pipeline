# 📦 MLOps Batch Pipeline

## 1. Project Overview

This project implements a minimal **MLOps-style batch pipeline** with the following features:

* Deterministic execution using configuration and fixed random seed
* Structured logging for observability
* Metrics generation for performance tracking
* Fully Dockerized for reproducible execution

---

## 2. How to Run Locally

Run the pipeline using:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

### Arguments:

* `--input` → Input dataset (CSV file)
* `--config` → Configuration file (YAML)
* `--output` → Output metrics file (JSON)
* `--log-file` → Log file for execution tracking

---

## 3. Docker Usage

Build and run the container:

```bash
docker build -t mlops-task .
docker run --rm mlops-task
```

---

## 4. Example Output

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4990,
  "latency_ms": 127,
  "seed": 42,
  "status": "success"
}
```

---

## 5. Design Decisions ⭐

* Rolling mean NaN values are excluded from signal computation to prevent bias
* Deterministic behavior is ensured using a fixed random seed
* Logging is written to both file and console for better observability
* Metrics are generated even in failure scenarios to improve reliability

---

## 🚀 Key Highlights

* Reproducible pipeline execution
* Clean separation of config, logic, and output
* Production-inspired MLOps practices

