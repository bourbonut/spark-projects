# About Dataset

This dataset is similar to 2015 Flight Delays and Cancellations. This dataset aims to incorporate multi-year data from 2009 to 2018 to offer additional time series insights.

[Link to the dataset](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018)

# Acknowledgements

All data files are downloaded from OST website, which stores flights on-time performance from 1987 to present. 

# Performance study between Spark and Polars

| Description                          | Duration for Spark | Duration for Polars  |
| ------------------------------------ | ------------------ | -------------------- |
| Departure delays computation         | 144.867264636 s    | 37.893018302999735 s |
| Arrival delays computation           | 5.292703053 s      | 33.98129544800031 s  |
| Delays by cities' computation        | 6.128966512 s      | 28.143728693999947 s |
| Delays by airlines' computation      | 2.91172382 s       | 28.30575065799985 s  |
| All (by starting computation again)  | 11.98073346 s      | 114.72786535500018 s |

Results can be found in `spark.txt` and `polars.txt` files.
