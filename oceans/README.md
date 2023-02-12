# Oceans project

The dataset can be found on the following link : [CLIWOC15.tar.gz](https://cloud.univ-grenoble-alpes.fr/s/5zjJaf5BStr4zw2/download/CLIWOC15.tar.gz).

Several questions are answered :

1. When running the provided example code, you will observe that there might be several entries that are equivalent. More specifically, you will see two entries for "British" with the only difference that one has an extra white space in the name. Propose a new version of the computation that will consider these two entries as the same one.
2. Count the total number of observations included in the dataset (each line corresponds to one observation)
3. Count the number of years over which observations have been made (Column `Year` should be used)
4. Display the oldest and the newest year of observation
5. Display the years with the minimum and the maximum number of observations (and the corresponding number of observations)
6. Count the distinct departure places (column `VoyageFrom`) using two methods (i.e., using the function distinct() or reduceByKey()) and compare the execution time.
7. Display the 10 most popular departure places
8. Display the 10 roads (defined by a pair `VoyageFrom` and `VoyageTo`) the most often taken.
- Here you can start by implementing a version where a pair `VoyageFrom`-`VoyageTo` A-B and a pair B-A correspond to different roads.
- Implement then a second version where A-B and B-A are considered as the same road.
9. Compute the hottest month (defined by column `Month`) on average over the years considering all temperatures (column `ProbTair`) reported in the dataset.
