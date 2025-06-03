def transform(df):
    df = df.dropna()
    df.columns = [c.lower() for c in df.columns]
    return df
