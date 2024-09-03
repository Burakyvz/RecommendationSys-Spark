from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def computeCosineSimilarity(spark, data):
    """
    Compute the cosine similarity between movie pairs.

    Parameters:
    - spark: The Spark session.
    - data: DataFrame containing movie pairs and their ratings.

    Returns:
    - DataFrame containing movie pairs with their similarity score and number of co-occurrences.
    """

    # Compute xx, yy, and xy columns for cosine similarity
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Compute numerator, denominator, and numPairs for each movie pair
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum("xy").alias("numerator"),
            (func.sqrt(func.sum('xx')) * func.sqrt(func.sum("yy"))).alias("denominator"),
            func.count("xy").alias("numPairs")
        )

    # Calculate cosine similarity score and select necessary columns
    result_ = calculateSimilarity.withColumn(
        "score",
        func.when(func.col("denominator") != 0,
                  func.col("numerator") / func.col("denominator")).otherwise(0)
    ).select("movie1", "movie2", "score", "numPairs")

    return result_

def getMovieName(movieNames, movieId):
    """
    Get the movie name for a given movie ID.

    Parameters:
    - movieNames: DataFrame containing movie IDs and their titles.
    - movieId: ID of the movie to retrieve the title for.

    Returns:
    - The title of the movie with the specified ID.
    """
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]
    return result[0]

def filterGoodMovies(movies, threshold=3.0):
    """
    Filter out movies with an average rating below a specified threshold.

    Parameters:
    - movies: DataFrame containing movie ratings.
    - threshold: The minimum average rating for a movie to be considered "good".

    Returns:
    - DataFrame with only the movies that have an average rating above the threshold.
    """
    movieAverages = movies.groupBy("movieID").agg(func.avg("rating").alias("avgRating"))
    goodMovies = movieAverages.filter(func.col("avgRating") >= threshold)
    return goodMovies

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieSimilarities") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for movie names
movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

# Define schema for movie ratings
moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load movie names with the defined schema
movieNames = spark.read \
    .option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("ml-100k/u.item")

# Load movie ratings with the defined schema
movies = spark.read \
    .option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("ml-100k/u.data")

# Filter out low-rated movies
goodMovies = filterGoodMovies(movies)

# Join good movies back to the ratings to filter out ratings for bad movies
ratings = movies.join(goodMovies, "movieID").select("userID", "movieID", "rating")

# Find all pairs of movies rated by the same user
moviePairs = ratings.alias("ratings1") \
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings2.userID") == func.col("ratings1.userID")) &
        (func.col("ratings2.movieID") > func.col("ratings1.movieID"))
    ) \
    .select(
        func.col("ratings1.movieID").alias("movie1"),
        func.col("ratings2.movieID").alias("movie2"),
        func.col("ratings1.rating").alias("rating1"),
        func.col("ratings2.rating").alias("rating2")
    )

# Compute similarities between movie pairs
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

# Prompt the user to input a movie ID
movieID = input("Enter the movie ID for which you want to find similar movies: ")

try:
    movieID = int(movieID)
except ValueError:
    print("Invalid input. Please enter a numeric movie ID.")
    sys.exit(1)

scoreThreshold = 0.97
coOccurrenceThreshold = 50.0

# Filter results for movies with high similarity and enough co-occurrences
filteredResults = moviePairSimilarities.filter(
    ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
    (func.col("score") > scoreThreshold) &
    (func.col("numPairs") > coOccurrenceThreshold)
)

# Sort by similarity score in descending order and take top 10
results = filteredResults.sort(func.col("score").desc()).take(10)

if results:
    print(f"Top 10 similar movies for {getMovieName(movieNames, movieID)}:\n")
    for result in results:
        similarMovieID = result.movie1 if result.movie1 != movieID else result.movie2
        print(f"{getMovieName(movieNames, similarMovieID)}\tscore: {result.score:.2f}\tstrength: {result.numPairs}")
else:
    print("No similar movies found for the specified movie ID.")
