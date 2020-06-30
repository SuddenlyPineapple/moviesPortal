from flask import Flask, jsonify
from GenresManager import GenresManager

app = Flask(__name__)
genresManager = GenresManager()


@app.route("/genres", methods=["GET"])
def genres_list():
    try:
        genres = genresManager.get_genres()
        return jsonify({'genres': genres}), 200, {"Content-Type": "application/json"}
    except:
        return jsonify({"INFO": "No genres"}), 404, {"Content-Type": "application/json"}


@app.route("/genres", methods=["POST"])
def add_genre():
    try:
        request_data = {"genresList": ['Action', 'Comedy']}
        return jsonify(request_data), 200, {"Content-Type": "application/json"}
    except:
        return jsonify({"INFO": "No genres"}), 404, {"Content-Type": "application/json"}


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5003)
