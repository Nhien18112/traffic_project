import argparse
from pathlib import Path
import math

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["Region index", "Date time"]).copy()

    # Core temporal features
    df["hour_of_day"] = df["Date time"].dt.hour
    df["day_of_week"] = df["Date time"].dt.dayofweek

    # Lag features per region
    for lag in [1, 2, 3]:
        df[f"speed_lag_{lag}"] = df.groupby("Region index")["Speed [kmh]"].shift(lag)

    # Rolling stats per region
    df["speed_roll_mean_3"] = (
        df.groupby("Region index")["Speed [kmh]"]
        .rolling(3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["target_t_plus_1"] = df.groupby("Region index")["Speed [kmh]"].shift(-1)
    return df.dropna().reset_index(drop=True)


def rolling_time_splits(df: pd.DataFrame, n_splits: int = 3, test_ratio: float = 0.2):
    n = len(df)
    test_size = int(n * test_ratio)
    train_start = 0

    for i in range(n_splits):
        train_end = n - test_size * (n_splits - i)
        test_end = train_end + test_size
        if test_end > n or train_end <= train_start:
            break
        yield i + 1, df.iloc[train_start:train_end], df.iloc[train_end:test_end]


def evaluate_naive_baseline(train_df: pd.DataFrame, test_df: pd.DataFrame):
    # Baseline đơn giản: dùng speed_lag_1 làm prediction.
    y_true = test_df["target_t_plus_1"]
    y_pred = test_df["speed_lag_1"]

    mae = mean_absolute_error(y_true, y_pred)
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    return mae, rmse


def run_backtest(csv_path: Path, n_splits: int):
    df = pd.read_csv(csv_path)

    if "Date time" not in df.columns and "Time" in df.columns:
        df = df.rename(columns={"Time": "Date time"})

    required_cols = {
        "Date time",
        "Region index",
        "Speed [kmh]",
        "Free flow speed [kmh]",
        "Congestion level [%]",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["Date time"] = pd.to_datetime(df["Date time"], errors="coerce")
    df = df.dropna(subset=["Date time", "Region index", "Speed [kmh]"])

    prepared = prepare_features(df)

    print(f"Prepared rows: {len(prepared)}")
    print(f"Rolling backtest results (naive baseline) for: {csv_path.name}")

    metrics = []

    for split_id, train_df, test_df in rolling_time_splits(prepared, n_splits=n_splits):
        mae, rmse = evaluate_naive_baseline(train_df, test_df)
        metrics.append((mae, rmse))
        print(
            f"Split {split_id}: train={len(train_df)} test={len(test_df)} | MAE={mae:.3f} RMSE={rmse:.3f}"
        )

    return metrics


def run_backtest_dir(data_dir: Path, n_splits: int):
    csv_files = sorted(
        p for p in data_dir.glob("*.csv") if "__MACOSX" not in str(p)
    )
    if not csv_files:
        raise ValueError(f"No CSV files found in {data_dir}")

    all_mae = []
    all_rmse = []

    for csv_file in csv_files:
        print("=" * 80)
        try:
            metrics = run_backtest(csv_file, n_splits)
            for mae, rmse in metrics:
                all_mae.append(mae)
                all_rmse.append(rmse)
        except Exception as exc:
            print(f"Skip {csv_file.name}: {exc}")

    if all_mae:
        print("=" * 80)
        print(
            f"Aggregate across datasets | MAE={sum(all_mae)/len(all_mae):.3f} RMSE={sum(all_rmse)/len(all_rmse):.3f}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Time-based rolling backtest for traffic dataset")
    parser.add_argument(
        "--csv",
        type=Path,
        required=False,
        help="Path to city traffic CSV (e.g. data/traffic-index-cities/Bangkok area, Thailand.csv)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=False,
        help="Directory of traffic city CSV files (e.g. data/traffic-index-cities)",
    )
    parser.add_argument("--splits", type=int, default=3, help="Number of rolling splits")
    args = parser.parse_args()

    if args.data_dir:
        run_backtest_dir(args.data_dir, args.splits)
    elif args.csv:
        run_backtest(args.csv, args.splits)
    else:
        raise ValueError("Provide either --csv or --data-dir")
