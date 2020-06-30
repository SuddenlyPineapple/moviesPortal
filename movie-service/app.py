from flask import Flask, jsonify, abort, request
from DataSet import DataSet
import sys


def create_app(n=1000, useRedis=True, useCassandra=True):
    app = Flask(__name__)

    dataSet = DataSet(
        rowsAmount=int(n),
        useRedis=useRedis,
        redisHost='redis',
        redisPort=6379,
        redisDB=0,
        useCassandra=useCassandra,
        cassandraHost='cassandra',
        cassandraPort=9042,
        importDataSetToCassandra=True
    )

    @app.route('/rating', methods=['POST'])
    def add_rating():
        if request.is_json:
            try:
                request_data = request.get_json()
                dataSet.append_row(request_data)
                return jsonify(request_data), 201, {"Content-Type": "application/json"}
            except:
                return jsonify("ERROR: Invalid JSON"), 422, {"Content-Type": "application/json"}
        else:
            return jsonify("ERROR: Invalid Format"), 415, {"Content-Type": "application/json"}

    @app.route('/rating', methods=['DELETE'])
    def remove_rating():
        if request.is_json:
            try:
                request_data = request.get_json()
                dataSet.delete_row(request_data['userID'], request_data['movieID'])
                return jsonify(request_data), 200, {"Content-Type": "application/json"}
            except:
                return jsonify("ERROR: Invalid JSON"), 422, {"Content-Type": "application/json"}
        else:
            return jsonify("ERROR: Invalid Format"), 415, {"Content-Type": "application/json"}

    @app.route('/ratings/<user_id>', methods=['GET'])
    def get_ratings(user_id):
        return jsonify(dataSet.get_dataset(user_id)), 200, {"Content-Type": "application/json"}

    @app.route('/avg-genre-ratings/all-users', methods=['GET'])
    def get_avg_genre_ratings_all_users():
        return jsonify(dataSet.get_avg_genres_rating().to_dict('records')[0]), 200, {"Content-Type": "application/json"}

    @app.route('/avg-genre-ratings/<user_id>', methods=['GET'])
    def get_avg_genre_ratings(user_id):
        response_data = dataSet.get_avg_genres_rating(user_id).to_dict('records')[0]
        response_data.update({'user_id': user_id})
        return jsonify(response_data), 200, {"Content-Type": "application/json"}

    @app.route('/user-profile/<user_id>', methods=['GET'])
    def get_avg_genre_ratings_user_difference_vector(user_id):
        response_data = dataSet.get_avg_genres_rating_user_difference_vector(user_id).to_dict('records')[0]
        response_data.update({'user_id': user_id})
        return jsonify(response_data), 200, {"Content-Type": "application/json"}

    @app.route("/test", methods=["POST"])
    def test():
        try:
            request_data = request.get_json()
            print("Hermes message:", request_data, file=sys.stderr)
            return jsonify(request_data), 200, {"Content-Type": "application/json"}
        except:
            abort(404)

    return app

def configure_app():
    from argparse import ArgumentParser
    from distutils import util

    global port, app
    parser = ArgumentParser()
    parser.add_argument('-useRedis', choices=[True, False], default='True', type=lambda x: bool(util.strtobool(x)))
    parser.add_argument('-useCassandra', choices=[True, False], default='True', type=lambda x: bool(util.strtobool(x)))
    parser.add_argument('-n')
    parser.add_argument('-port')
    args = parser.parse_args()
    useRedis = args.useRedis if args.useRedis is not None else True
    useCasandra = args.useCassandra if args.useCassandra is not None else True
    n = args.n if args.n is not None else 0
    port = args.port if args.port is not None else 5000
    app = create_app(n, useRedis, useCasandra)

    return app, port

if __name__ == '__main__':
    app, port = configure_app()
    app.run(host='0.0.0.0', debug=True, port=5000)
