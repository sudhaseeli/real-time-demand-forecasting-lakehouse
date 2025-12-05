import pathlib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib

DATA_PATH = pathlib.Path(__file__).resolve().parents[0] / "data" / "daily_product_sales.csv"
MODEL_DIR = pathlib.Path(__file__).resolve().parents[0] / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

def build_lag_features(df: pd.DataFrame, lags=3) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df = df.sort_values(["product_id", "order_date"])
    for lag in range(1, lags + 1):
        df[f"lag_{lag}"] = df.groupby("product_id")["total_quantity"].shift(lag)
    df = df.dropna()
    return df

def main():
    if not DATA_PATH.exists():
        print(f"Feature data not found at {DATA_PATH}")
        return
    df = pd.read_csv(DATA_PATH)
    df_lagged = build_lag_features(df)
    feature_cols = ["lag_1", "lag_2", "lag_3"]
    X = df_lagged[feature_cols]
    y = df_lagged["total_quantity"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
    print(f"Model R^2 on test set: {score:.3f}")
    model_path = MODEL_DIR / "demand_forecast_model.joblib"
    joblib.dump(model, model_path)
    print(f"Saved model to {model_path}")

if __name__ == "__main__":
    main()
