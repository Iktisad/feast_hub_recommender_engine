from flask import Flask, jsonify
from restaurantcf import *

app = Flask(__name__)

@app.route("/")
def recommendationEngineStart():
    return jsonify({'message': 'welcome to my api'})

if __name__ == '__main__':
    app.run(port=3000,debug=True)