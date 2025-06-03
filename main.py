from etl.extract import extract
from etl.transform import transform
from etl.load import load

def run_etl():
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__":
    run_etl()
