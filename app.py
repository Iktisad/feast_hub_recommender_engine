from flask import Flask, jsonify
from src.restaurantcf import RecommenderEngine


app = Flask(__name__)
engine: RecommenderEngine = RecommenderEngine()

@app.route("/")
def home():
    return jsonify({'message': 'Server is working'})
    # pass

@app.route("/<userID>", methods=['GET'])
def getRestaurantRecommendation(userID):
    # return jsonify({'user': userID})
    try:
        data = engine.getRecommendation(userID)
        return jsonify({
                'message': 'Displaying results',
                'status': 'success',
                'data': data
            })
    except Exception as error:
        print(error)
        return jsonify({
            'message': 'Internal Server error in Recommender Engine',
            'status': 'failed'
        })

if __name__ == '__main__':
    # start the recommender
    app.run(host ='localhost',port=3000,debug=True)