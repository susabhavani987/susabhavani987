import pandas as pd

def extract(file_path="data/input.csv"):
    return pd.read_csv(file_path)
