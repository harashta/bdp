from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadDB():
    db = {}
    with open("taxi/train.csv") as f:
        for line in f:
            fields = line.split(',')
            db[int(fields[0])] = fields[1]
    return db

def parseInput(line):
    fields = line.split()
    return Row(trip_id = str(fields[0]), call_type = char(fields[1]), origin_call = int(fields[2]), origin_stand = int(fields[3]), taxi_id = int(fields[4]), timestamp = int(fields[5]), day_type = char(fields[6]), missing_data = str(fields[7]), polyline = (fields[8]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("TaxiOptimalPath").getOrCreate()

    # Load up our movie ID -> name dictionary
    db = loaddb()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/taxi/test.csv")
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    taxiDataset = spark.createDataFrame(movies)

    ####################################

    for i in taxiDataset:
        print (db[i[0]], i[1], i[2])

    # # Compute average rating for each movieID
    # averageRatings = taxiDataset.groupBy("movieID").avg("rating")

    # # Compute count of ratings for each movieID
    # counts = taxiDataset.groupBy("movieID").count()

    # # Join the two together (We now have movieID, avg(rating), and count columns)
    # averagesAndCounts = counts.join(averageRatings, "movieID")

    # # Pull the top 10 results
    # topTen = averagesAndCounts.orderBy("avg(rating)").filter("count > 10").take(10)

    # # Print them out, converting movie ID's to names as we go.
    # for movie in topTen:
    #     print (db[movie[0]], movie[1], movie[2])

    # # Stop the session
    spark.stop()
