import pathlib
import pandas as pd
import joblib
from datetime import timedelta

DATA_PATH = pathlib.Path(__file__).resolve().parents[0] / "data" / "daily_product_sales.csv"
MODEL_PATH = pathlib.Path(__file__).resolve().parents[0] / "models" / "demand_forecast_model.joblib"
FORECAST_OUTPUT = pathlib.Path(__file__).resolve().parents[0] / "output"
FORECAST_OUTPUT.mkdir(parents=True, exist_ok=True)

def prepare_latest_lags(df: pd.DataFrame, lags=3):
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    latest = df.sort_values(["product_id", "order_date"]).groupby("product_id").tail(lags)
    pivot = latest.pivot(index="product_id", columns="order_date", values="total_quantity")
    pivot = pivot.sort_index(axis=1)
    if pivot.shape[1] < lags:
        return None
    last_cols = pivot.columns[-lags:]
    X = pivot[last_cols]
    X.columns = [f"lag_{i}" for i in range(1, lags + 1)]
    return X

def main():
    if not DATA_PATH.exists() or not MODEL_PATH.exists():
        print("Missing data or model for forecasting.")
        return
    df = pd.read_csv(DATA_PATH)
    X_latest = prepare_latest_lags(df)
    if X_latest is None:
        print("Not enough history for forecasting.")
        return
    model = joblib.load(MODEL_PATH)
    preds = model.predict(X_latest)
    product_ids = X_latest.index.tolist()
    last_date = pd.to_datetime(df["order_date"]).max()
    forecast_date = last_date + timedelta(days=1)
    out_df = pd.DataFrame({
        "product_id": product_ids,
        "forecast_date": forecast_date,
        "forecast_quantity": preds
    })
    out_csv = FORECAST_OUTPUT / "daily_demand_forecast.csv"
    out_df.to_csv(out_csv, index=False)
    print(f"Wrote forecasts to {out_csv}")

if __name__ == "__main__":
    main()
