import sqlite3

def load(df, db_path="output.db"):
    conn = sqlite3.connect(db_path)
    df.to_sql("cleaned_data", conn, if_exists="replace", index=False)
    conn.close()
