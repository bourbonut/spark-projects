import polars as pl
from time import perf_counter as pf

pl.Config.set_tbl_rows(20)

df = pl.scan_csv("*.csv")
df = df.select(
    [
        pl.col("FL_DATE")
        .str.strptime(pl.Datetime, "%Y-%m-%d")
        .dt.month()
        .alias("MONTH"),
        pl.col("ORIGIN"),
        pl.col("OP_CARRIER"),
        pl.col("DEP_DELAY").cast(pl.Float64),
        pl.col("ARR_DELAY").cast(pl.Float64),
    ]
).filter(pl.col("ARR_DELAY") > 0)

start = pf()
print(df.groupby("MONTH").agg(pl.col("DEP_DELAY").sum()).collect())
duration1 = pf() - start

start = pf()
print(df.groupby("MONTH").agg(pl.col("ARR_DELAY").sum()).collect())
duration2 = pf() - start

start = pf()
print(df.groupby("ORIGIN").agg(pl.col("ARR_DELAY").sum().alias("sum")).sort("sum").collect())
duration3 = pf() - start

start = pf()
print(df.groupby("OP_CARRIER").agg(pl.col("ARR_DELAY").sum().alias("sum")).sort("sum").collect())
duration4 = pf() - start

start = pf()
print(df.groupby("MONTH").agg(pl.col("DEP_DELAY").sum()).collect())
print(df.groupby("MONTH").agg(pl.col("ARR_DELAY").sum()).collect())
print(df.groupby("ORIGIN").agg(pl.col("ARR_DELAY").sum().alias("sum")).sort("sum").collect())
print(df.groupby("OP_CARRIER").agg(pl.col("ARR_DELAY").sum().alias("sum")).sort("sum").collect())
duration5 = pf() - start

print(f"[DataFrame] Duration for departure delays computation = {duration1} s")
print(f"[DataFrame] Duration for arrival delays computation = {duration2} s")
print(f"[DataFrame] Duration for delays by cities computation = {duration3} s")
print(f"[DataFrame] Duration for delays by airlines computation = {duration4} s")
print(f"[DataFrame] Duration for all computation = {duration5} s")
