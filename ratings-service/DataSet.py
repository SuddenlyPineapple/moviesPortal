import pandas as pd
import numpy as np
import json
from pandas.io.json import json_normalize
from pyhermes import publish

from Utils.CassandraClient import CassandraClient
from Utils.RedisClient import RedisClient
import pyhermes
from pyhermes.settings import DEFAULTS


class DataSet:
    def __init__(self, rowsAmount=1000, useRedis=False, redisHost='redis', redisPort=6379, redisDB=0,
                 useCassandra=False, cassandraHost='cassandra', cassandraPort=9042, importDataSetToCassandra=False):
        self.rowsAmount = rowsAmount
        self.useRedis = useRedis
        self.useCassandra = useCassandra
        self.data_set = self.load_and_merge_sets(rowsAmount)

        # Hermes Configuration
        DEFAULTS["BASE_URL"] = 'http://frontend:8080'
        DEFAULTS["PUBLISHING_GROUP"] = {
            'groupName': 'test'
        }

        if useCassandra:
            print("Cassandra Configured at ", cassandraHost, ":", cassandraPort)
            self.cassandraClient = CassandraClient(cassandraHost, cassandraPort)
            if importDataSetToCassandra:
                self.data_set = self.data_set.fillna(0)
                for key, row in self.data_set.iterrows():
                    genres = {k: row[k] for k in set(list(row.keys())) - {'userID', 'movieID', 'rating'}}
                    print(genres)
                    self.cassandraClient.insert(
                        np.int64(row.get('userID')),
                        np.int64(row.get('movieID')),
                        row.get('rating'),
                        genres
                    )

        if useRedis:
            print("Redis Configured at ", redisHost, ":", redisPort)
            self.redisClient = RedisClient(redisHost, redisPort, redisDB)
            self.get_avg_genres_rating()

    def load_and_merge_sets(self, nrows):
        movie_set = pd.DataFrame(
            pd.read_csv(
                open('data/user_ratedmovies.dat'),
                delimiter="\t",
                header=0,
                usecols=['movieID', 'userID', 'rating'],
                nrows=nrows
            )
        )
        genres_set = pd.DataFrame(pd.read_csv(open('data/movie_genres.dat'), delimiter="\t", header=0))

        genres_set['dummyColumn'] = 1

        genres_set_pivoted = genres_set.pivot_table(
            index="movieID",
            columns="genre",
            values="dummyColumn"
        )
        # genres_set_pivoted = genres_set_pivoted.fillna(0) #it breaks average function
        genres_set_pivoted = genres_set_pivoted.add_prefix('genre-')

        self.genres = genres_set_pivoted

        movies_with_genred_set = pd.merge(movie_set, genres_set_pivoted, on="movieID")
        # print(movies_with_genred_set)
        return movies_with_genred_set

    def get_dataset(self, user_id=None):
        if self.useCassandra:
            return self.cassandraClient.get(int(user_id))

        returned_set = self.data_set['userID' == np.int64(user_id)].fillna(0)
        return returned_set.to_dict('records')

    @pyhermes.publisher(topic='test.test-topic', auto_publish_result=True)
    def append_row(self, request_data):
        # print(self.data_set)
        if self.useCassandra:
            genres = {k: request_data[k] for k in set(list(request_data.keys())) - {'userID', 'movieID', 'rating'}}
            print(genres)
            self.cassandraClient.insert(
                request_data.get('userID'),
                request_data.get('movieID'),
                request_data.get('rating'),
                genres
            )
        df = json_normalize(request_data)
        rating_with_genres = pd.merge(df, self.genres, on="movieID")
        print(rating_with_genres)
        self.data_set = self.data_set.append(rating_with_genres, ignore_index=True)
        # print(self.data_set)
        return request_data

    def delete_row(self, userID, movieID):
        if self.useCassandra:
            self.cassandraClient.delete(userID, movieID)

        index_ratings = self.data_set[
            (self.data_set['userID'] == userID) &
            (self.data_set['movieID'] == movieID)
            ].index
        self.data_set.drop(index_ratings, inplace=True)
        print(self.data_set)
        return self.data_set.to_dict('records')

    def get_avg_genres_rating(self, user_id=None):
        # All users Redis Cache use
        if self.useRedis and user_id is None:
            redisRating = self.redisClient.get("all_users_rating")
            if redisRating is not None:
                avg_rating = pd.DataFrame(columns=self.genres.columns).from_dict(json.loads(redisRating))
                if self.useCassandra:
                    return avg_rating
                if len(self.data_set.index) == avg_rating['ratings_count'][0]:
                    return avg_rating
        if self.useRedis and user_id is not None:
            redisRating = self.redisClient.get("user_rating_" + str(user_id))
            if redisRating is not None:
                avg_user_rating = pd.DataFrame(columns=self.genres.columns).from_dict(json.loads(redisRating))
                if self.useCassandra:
                    return avg_user_rating
                if len(self.data_set[
                           self.data_set['userID'
                           ] == np.int64(user_id)].index) == avg_user_rating['ratings_count'][0]:
                    return avg_user_rating

        if self.useCassandra:
            if user_id is None:
                self.data_set = pd.DataFrame(self.cassandraClient.get()).replace({0.0: np.nan})
            else:
                self.data_set = pd.DataFrame(self.cassandraClient.get(np.int64(user_id))).replace({0.0: np.nan})

        try:
            avg_rating = self.calculate_avg(user_id)

            # Redis Update Switch (user/all/updated-values)
            if self.useRedis and user_id is None:
                self.redisClient.set("all_users_rating", avg_rating.to_json(orient='columns'))
                if self.useCassandra:
                    self.redisClient.expire("all_users_rating", 240)
            if self.useRedis and user_id is not None:
                self.redisClient.set("user_rating_" + str(user_id), avg_rating.to_json(orient='columns'))
                if self.useCassandra:
                    self.redisClient.expire("user_rating_" + str(user_id), 60)

            return avg_rating
        except:
            return pd.DataFrame({'message': ['No ratings']})

    @pyhermes.publisher(topic='test.test-topic')
    def calculate_avg(self, user_id):
        # User/all_users rating switch
        if user_id is None:
            rating_vector = self.data_set["rating"].values
            genres_array = self.data_set[self.genres.columns].values
        else:
            rating_vector = self.data_set.loc[
                self.data_set['userID'] == np.int64(user_id),
                ["rating"]
            ].values
            genres_array = self.data_set.loc[
                self.data_set['userID'] == np.int64(user_id),
                self.genres.columns
            ].values
        # Rating Calculating
        rating_array = rating_vector.reshape(rating_vector.shape[0], 1)
        ratings_to_genres = rating_array * genres_array
        avg_ratings_to_genres = np.nan_to_num(
            np.nanmean(ratings_to_genres, axis=0)
        )
        avg_rating = pd.DataFrame(
            avg_ratings_to_genres.reshape(1, avg_ratings_to_genres.shape[0]),
            columns=self.genres.columns
        )
        avg_rating['ratings_count'] = len(rating_vector)
        publish(self.calculate_avg._topic, avg_rating.to_dict('records')[0])
        return avg_rating

    def get_avg_genres_rating_user_difference_vector(self, userID):
        try:
            # TODO: Naprawić ujemny ratings_count
            return self.get_avg_genres_rating(userID).sub(
                self.get_avg_genres_rating().squeeze()
            )
        except:
            return pd.DataFrame({'message': ['No ratings']})


if __name__ == "__main__":
    # dataSet = DataSet(1000)
    # dataSet = DataSet(rowsAmount=1000, useRedis=True, redisHost='redis', redisPort=6379, redisDB=0)
    dataSet = DataSet(rowsAmount=10, useRedis=True, redisHost='localhost', redisPort=6379, redisDB=0,
                      useCassandra=True, cassandraHost='localhost', cassandraPort=9042)

    # print(dataSet.get_dataset())
    dataSet.append_row({
        "userID": 75,
        "movieID": 906,
        "rating": 5.0,
        "genre-Action": 1,
        "genre-Adventure": 0,
        "genre-Animation": 0,
        "genre-Children": 0,
        "genre-Comedy": 0,
        "genre-Crime": 0,
        "genre-Documentary": 0,
        "genre-Drama": 1,
        "genre-Fantasy": 0,
        "genre-Film-Noir": 0,
        "genre-Horror": 0,
        "genre-IMAX": 0,
        "genre-Musical": 0,
        "genre-Mystery": 1,
        "genre-Romance": 1,
        "genre-Sci-Fi": 0,
        "genre-Short": 0,
        "genre-Thriller": 1,
        "genre-War": 0,
        "genre-Western": 0
    })

    dataSet.delete_row(userID=75, movieID=906)

    a = dataSet.get_avg_genres_rating()
    print(a)
    b = dataSet.get_avg_genres_rating(user_id=78)
    print(b)
    c = dataSet.get_avg_genres_rating_user_difference_vector(userID=78)
    print(c)
    print("End")
