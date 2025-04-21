from flask import Flask, jsonify, request
from google.cloud import datastore

app = Flask(__name__)
datastore_client = datastore.Client()

@app.route('/businesses', methods=['POST'])
def create_business():
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing one of the required fields'}), 400
    
    business_key = datastore_client.key('Business')
    business = datastore.Entity(key=business_key)
    business.update(data)
    datastore_client.put(business)
    business['id'] = business.key.id
    return jsonify(business), 201

@app.route('/businesses', methods=['GET'])
def get_businesses():
    query = datastore_client.query(kind='Business')
    businesses = list(query.fetch())
    for business in businesses:
        business['id'] = business.key.id
    return jsonify(businesses), 200

@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business(business_id):
    business_key = datastore_client.key('Business', business_id)
    business = datastore_client.get(business_key)

    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    return jsonify(business), 200

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)