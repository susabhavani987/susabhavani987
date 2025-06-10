import sqlite3
import shutil

def load(df, db_path="output.db"):
    shutil.copy("./output.db", "/tmp/output.db")
    conn = sqlite3.connect("/tmp/output.db")
    ##conn = sqlite3.connect(db_path)
    df.to_sql("cleaned_data", conn, if_exists="replace", index=False)
    conn.close()
