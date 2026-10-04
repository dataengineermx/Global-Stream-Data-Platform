import pandas as pd

REQUIRED_COLUMNS = [
    "type",
    "status",
    "magnitude",
    "place",
    "longitude",
    "latitude",
    "depth",
    "time"
]

def validate(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Check required columns
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # 2. Remove duplicate records
    df = df.drop_duplicates()

    # 3. Remove rows with required null values
    df = df.dropna(
        subset=["type", "status", "place","time", "longitude", "latitude", "magnitude"]
    )

    # 4. Validate coordinate ranges
    df = df[df["longitude"].between(-180, 180)]
    df = df[df["latitude"].between(-90, 90)]

    # 5. Depth should not be negative
    df = df[df["depth"] >= 0]

    # 6. Magnitude should be reasonable
    df = df[df["magnitude"].between(-2, 10)]

    return df