from flask import Flask, request, jsonify
import joblib

app = Flask(__name__)

# Load the serialized model
model = joblib.load('model.pkl')

@app.route('/predict', methods=['POST'])
def predict():
    # Get the data from the POST request.
    data = request.get_json(force=True)

    # Make prediction using the model
    input_data = preprocess_input(data)
    prediction = model.predict(input_data)
    return jsonify(prediction)

if __name__ == '__main__':
    app.run(debug=True)
