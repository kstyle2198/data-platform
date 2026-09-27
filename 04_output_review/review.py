import pandas as pd

path  = "./data/00000-2-831b89de-ee23-46d9-9b8c-8894e8cd5170-0-00001.parquet"

df = pd.read_parquet(path, engine="pyarrow")

print("=== 데이터 ===")
print(df)

print("\n=== 컬럼 ===")
print(df.columns)

print("\n=== 데이터 타입 ===")
print(df.dtypes)

print("\n=== 행/열 개수 ===")
print(df.shape)

print("\n=== 앞 5개 ===")
print(df.head())