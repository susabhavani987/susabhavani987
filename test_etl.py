from etl.transform import transform
import pandas as pd

def test_transform():
    df = pd.DataFrame({"Name": ["Alice", None]})
    result = transform(df)
    assert result.shape[0] == 1
    assert 'name' in result.columns
