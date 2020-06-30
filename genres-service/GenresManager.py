import pandas as pd
import pyhermes
from pyhermes.settings import DEFAULTS


class GenresManager:
    def __init__(self):
        genres_set = pd.DataFrame(pd.read_csv(open('data/movie_genres.dat'), delimiter="\t", header=0))
        self.genres_list = genres_set['genre'].unique()
        # Hermes Configuration
        DEFAULTS["BASE_URL"] = 'http://frontend:8080'
        DEFAULTS["PUBLISHING_GROUP"] = {
            'groupName': 'moviePortal'
        }
        print('OK')

    def get_genres(self):
        return self.genres_list

    @pyhermes.publisher(topic='moviePortal.add-genre', auto_publish_result=True)
    def add_genre(self, genre):
        self.genres_list.append(genre)
        return self.get_genres()


if __name__ == '__main__':
    genresManager = GenresManager()
    print(genresManager.get_genres())
