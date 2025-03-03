## Necessary library for manipulating
import json
import pandas as pd

## Important setting for manipulating
pd.set_option("display.max_columns", None)

## Import data for manipulating
# CSV file
trans_df = pd.read_csv("./.data/transactions_data.csv")

# JSON file
with open("./.data/frauds_data.json", mode="r") as frauds_file:
    frauds_data = json.load(frauds_file)
with open("./.data/mccs_data.json", mode="r") as mccs_file:
    mccs_data = json.load(mccs_file)

## Manipulate for frauds_data.json (Dictionary of dictionary)
# Create frauds_data to dataframe
frauds_keys = list(frauds_data["target"].keys())
frauds_values = list(frauds_data["target"].values())
frauds_df = pd.DataFrame({"id": frauds_keys, "fraud_status": frauds_values})

# Check missing value in dataframe
print(f"\nMissing value in dataframe:\n{frauds_df.isnull().sum()}")

# Check dimension of dataframe
print(f"\nDimension of dataframe:\n{frauds_df.shape}")

# Check datatypes of dataframe
print(f"\nDatatypes of dataframe:\n{frauds_df.dtypes}")

# Convert fraud_id to numeric in dataframe
frauds_df["id"] = pd.to_numeric(frauds_df["id"])

# Recheck datatypes in dataframe
print(f"\nRecheck datatypes in dataframe:\n{frauds_df.dtypes}")

## Manipulate for mccs_data.json (Dictionary)
# Create mccs_data to dataframe
mccs_keys = list(mccs_data.keys())
mccs_values = list(mccs_data.values())
mccs_df = pd.DataFrame({"mcc": mccs_keys, "mcc_detail": mccs_values})

# Check missing value in dataframe
print(f"\nMissing value in dataframe:\n{mccs_df.isnull().sum()}")

# Check dimension of dataframe
print(f"\nDimension of dataframe:\n{mccs_df.shape}")

# Check datatypes of dataframe
print(f"\nDatatypes of dataframe:\n{mccs_df.dtypes}")

# Convert mcc_id to numeric in dataframe
mccs_df["mcc"] = pd.to_numeric(mccs_df["mcc"])

# Recheck datatypes in dataframe
print(f"\nRecheck datatypes in dataframe:\n{mccs_df.dtypes}")

## Model predictive rate of binary classification
model_rate = (len(frauds_df["id"]) / len(trans_df["id"])) * 100
print(f"\nModel predictive rate: {round(model_rate, 2)}%")

## Manipulate for transaction_data.csv (Dataframe)
# Merge frauds_df and mccs_df to dataframe
trans_df = pd.merge(trans_df, frauds_df, how="inner", on="id")
trans_df = pd.merge(trans_df, mccs_df, how="inner", on="mcc")

# Check missing value in dataframe
print(f"\nMissing value in dataframe:\n{trans_df.isnull().sum()}")

# Check dimension of dataframe
print(f"\nDimension of dataframe:\n{trans_df.shape}")

# Check datatypes of dataframe
print(f"\nDatatypes of dataframe:\n{trans_df.dtypes}")

# Fill missing value at merchant_state
trans_df.fillna({"merchant_state": "None"}, inplace=True)

# Fill missing value at zip
trans_df.fillna({"zip": trans_df["zip"].mean()}, inplace=True)

# Fill missing value at errors
trans_df.fillna({"errors": "None"}, inplace=True)

# Convert date to datetime in dataframe
trans_df["date"] = pd.to_datetime(trans_df["date"])

# Convert amount to numeric in dataframe
trans_df["amount"] = trans_df["amount"].str.replace("$", "")
trans_df["amount"] = pd.to_numeric(trans_df["amount"])

# Recheck missing value in dataframe
print(f"\nRecheck missing value in dataframe:\n{trans_df.isnull().sum()}")

# Recheck datatypes in dataframe
print(f"\nRecheck datatypes in dataframe:\n{trans_df.dtypes}")

# Change series name in dataframe
trans_df.rename(columns={"id": "tran_id", "date": "act_date", "mcc": "mcc_id"}, inplace=True)

## Sample data for checking
print(f"\nSample data in dataframe:\n{trans_df.head(3)}")
