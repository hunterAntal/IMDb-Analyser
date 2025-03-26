from pymongo import MongoClient

def get_top_movies_by_genre(genre):
    client = MongoClient("mongodb://localhost:27017") # connect to the database
    db = client['3675Local'] # connect to the database

    pipeline = [
        # Filter basics first to reduce the number of documents before join
        {
            "$match": {
                "titleType": "movie",
                "isAdult": 0,
                "genres": genre  # Since genres is an array, this checks for inclusion
            }
        },
        # Join ratings data from the ratings collection
        {
            "$lookup": {
                "from": "title.ratings_cleaned",
                "localField": "tconst",
                "foreignField": "tconst",
                "as": "rating_info"
            }
        },
        # Flatten the rating_info array
        { "$unwind": "$rating_info" },
        # Further filter on the joined rating info for minimum votes
        {
            "$match": {
                "rating_info.numVotes": { "$gte": 1000 }
            }
        },
        # Sort by the average rating in descending order
        {
            "$sort": { "rating_info.averageRating": -1 }
        },
        # Limit to the top 5 results
        {
            "$limit": 5
        },
        # Project only the fields you need
        {
            "$project": {
                "_id": 0,
                "title": "$primaryTitle",
                "rating": "$rating_info.averageRating",
                "votes": "$rating_info.numVotes",
                "year": "$startYear"
            }
        }
    ]

    results = list(db.title.basics_cleaned.aggregate(pipeline))
    return results

# Example usage:
if __name__ == "__main__":
    top_comedy_movies = get_top_movies_by_genre("Comedy")
    for movie in top_comedy_movies:
        print(movie)
