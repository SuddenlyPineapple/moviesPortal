from functools import reduce

from cassandra.cluster import Cluster


class CassandraClient:
    def __init__(self, host="localhost", port=9042):
        self.keyspace = "user_ratings"
        self.table = "user_ratedmovies"

        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()

        self.create_keyspace()
        self.create_table()

        self.lastindex = 0

    def create_keyspace(self):
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS " + self.keyspace + " WITH replication = { 'class': "
                                                                                "'SimpleStrategy', "
                                                                                "'replication_factor': "
                                                                                "'1' }")

    def create_table(self):
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS " + self.keyspace + "." + self.table + "(userID int, movieID int, "
                                                                               "rating float, "
                                                                               "genres map<text, float>, "
                                                                               "PRIMARY KEY(userID, movieID)) "
        )

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")
        self.lastindex = 0

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")
        self.lastindex = 0

    def get(self, user_id=None):
        if user_id is None:
            rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        else:
            rows = self.session.execute(
                "SELECT * FROM " + self.keyspace + "." + self.table + " WHERE userID=%(userID)s;", {
                    'userID': user_id
                })

        table_data = []

        for row in rows:
            table_data.append(
                reduce(lambda x, y: dict(x, **y), ({
                                                       "userID": row.userid,
                                                       "movieID": row.movieid,
                                                       "rating": row.rating,
                                                   }, dict(row.genres))))
        return table_data

    def insert(self, userID, movieID, rating, genres={}):
        if genres is None:
            genres = {}
        self.session.execute(
            "INSERT INTO " + self.keyspace + "." + self.table + "(userID, movieID, rating, genres) VALUES (%("
                                                                "userID)s, %(movieID)s, %(rating)s, %(genres)s)",
            {
                'userID': userID,
                'movieID': movieID,
                'rating': rating,
                'genres': genres
            })
        self.lastindex = len(self.get())

    def delete(self, userID, movieID):
        self.session.execute(
            "DELETE FROM " + self.keyspace + "." + self.table + " WHERE userID=%(userID)s AND movieID=%(movieID)s",
            {
                'userID': userID,
                'movieID': movieID
            })


if __name__ == "__main__":
    client = CassandraClient(host='localhost', port=9042)
    client.delete_table()
    client.create_keyspace()
    client.create_table()

    print(client.get())
    client.insert(1, 2, 2.0, {'genre-Action': 1})
    print(client.get())
    client.clear_table()
    print(client.get())
