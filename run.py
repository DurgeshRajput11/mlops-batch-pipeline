import argparse
import logging
import yaml
import pandas as pd
import numpy as np
import json
import time
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    return parser.parse_args()


def setup_logger(log_file):
    logger = logging.getLogger("mlops")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def load_config(path):
    if not os.path.exists(path):
        raise ValueError("Config file not found")

    try:
        with open(path, "r") as f:
            config = yaml.safe_load(f)
    except Exception:
        raise ValueError("Invalid YAML format")

    required = ["seed", "window", "version"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing config field: {key}")

    if not isinstance(config["seed"], int):
        raise ValueError("seed must be int")

    if not isinstance(config["window"], int) or config["window"] <= 0:
        raise ValueError("window must be positive int")

    if not isinstance(config["version"], str):
        raise ValueError("version must be string")

    return config


def load_data(path):
    if not os.path.exists(path):
        raise ValueError("Input file not found")

    try:
        df = pd.read_csv(path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("Empty dataset")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df


def process(df, window):
    df["rolling_mean"] = df["close"].rolling(window).mean()

    valid_mask = df["rolling_mean"].notna()

    df["signal"] = 0
    df.loc[valid_mask, "signal"] = (
        df.loc[valid_mask, "close"] > df.loc[valid_mask, "rolling_mean"]
    ).astype(int)

    return df, valid_mask


def compute_metrics(df, valid_mask, version, seed, start_time):
    signal_rate = df.loc[valid_mask, "signal"].mean()

    latency_ms = int((time.perf_counter() - start_time) * 1000)

    return {
        "version": version,
        "rows_processed": len(df),
        "metric": "signal_rate",
        "value": round(float(signal_rate), 4),
        "latency_ms": latency_ms,
        "seed": seed,
        "status": "success"
    }



def write_metrics(path, metrics):
    with open(path, "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    args = parse_args()
    logger = setup_logger(args.log_file)

    start_time = time.perf_counter()

    try:
        logger.info("Job started")

        config = load_config(args.config)
        logger.info(f"Config loaded: {config}")

        np.random.seed(config["seed"])

        df = load_data(args.input)
        logger.info(f"Rows loaded: {len(df)}")

        df, valid_mask = process(df, config["window"])
        logger.info("Processing completed (rolling mean + signal)")

        metrics = compute_metrics(
            df, valid_mask, config["version"], config["seed"], start_time
        )

        write_metrics(args.output, metrics)

        logger.info(f"Metrics: {metrics}")
        logger.info("Job completed successfully")

        print(json.dumps(metrics))
        sys.exit(0)

    except Exception as e:
        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)

        logger.error(f"Error: {str(e)}")
        print(json.dumps(error_metrics))

        sys.exit(1)


if __name__ == "__main__":
    main()