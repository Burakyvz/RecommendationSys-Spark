# Movie Similarity Finder using PySpark

This project provides a PySpark-based solution for finding movie recommendations based on cosine similarity of user ratings. The script calculates the similarity between pairs of movies by analyzing user rating patterns and suggests movies that are similar to a given movie. The dataset used for this project is the [MovieLens 100k dataset](https://grouplens.org/datasets/movielens/100k/), which contains 100,000 ratings from 943 users on 1,682 movies.

## Key Features

- **Cosine Similarity Calculation**: Computes cosine similarity between movie pairs based on user ratings to find similar movies.
- **Interactive Movie Selection**: Allows users to input a movie ID and returns the top 10 most similar movies.
- **Filtering of Low-Rated Movies**: Filters out movies with low average ratings to ensure recommendations are of high quality.
- **Optimized Performance**: Leverages Apache Spark's distributed computing capabilities for efficient data processing.


## Example Output

When you run the script, you'll be prompted to enter a movie ID. Based on the input movie ID, the script computes and displays the top 10 similar movies, their similarity scores, and the number of co-occurrences (strength).

Example:

```plaintext
Enter the movie ID for which you want to find similar movies: 23
Top 10 similar movies for Taxi Driver (1976):

L.A. Confidential (1997)    score: 0.98    strength: 67
12 Angry Men (1957)         score: 0.98    strength: 65
Psycho (1960)               score: 0.98    strength: 117
Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963) score: 0.98    strength: 102
Chinatown (1974)            score: 0.98    strength: 93
Godfather, The (1972)       score: 0.98    strength: 142
Godfather: Part II, The (1974) score: 0.98 strength: 115
Rear Window (1954)          score: 0.98    strength: 104
Glory (1989)                score: 0.98    strength: 74
North by Northwest (1959)   score: 0.98    strength: 91
