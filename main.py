from flask import Flask, jsonify, request
from google.cloud import datastore

app = Flask(__name__)
datastore_client = datastore.Client()

@app.route('/businesses', methods=['POST'])
def create_business():
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400
    
    business_key = datastore_client.key('Business')
    business = datastore.Entity(key=business_key)
    business.update(data)
    datastore_client.put(business)

    # include the id in the response
    business['id'] = business.key.id

    return jsonify(business), 201

@app.route('/businesses', methods=['GET'])
def get_businesses():
    query = datastore_client.query(kind='Business')
    businesses = list(query.fetch())

    # include the id in the response
    for business in businesses:
        business['id'] = business.key.id

    return jsonify(businesses), 200

@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business_by_id(business_id):
    business_key = datastore_client.key('Business', business_id)
    business = datastore_client.get(business_key)

    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    return jsonify(business), 200

@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400
    
    # Get entry from Datastore
    business_key = datastore_client.key('Business', business_id)
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    business.update(data)
    datastore_client.put(business)
    
    # include the id in the response
    business['id'] = business_id
    
    return jsonify(business), 200

@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    business_key = datastore_client.key('Business', business_id)
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    # Get and delete all of the business reviews 
    query = datastore_client.query(kind='Review')
    query.add_filter(filter=('business_id', '=', business_id))
    reviews = query.fetch()
    for review in reviews:
        datastore_client.delete(review.key)
    
    datastore_client.delete(business_key)

    # just return the status code 204 (No Content [Success])
    return '', 204

@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_owner_businesses(owner_id):
    query = datastore_client.query(kind='Business')
    query.add_filter(filter=('owner_id', '=', owner_id))
    businesses = list(query.fetch())

    # Add ID to the response
    for buiness in businesses:
        buiness['id'] = buiness.key.id

    return jsonify(businesses), 200

@app.route('/reviews', methods=['POST'])
def create_review():
    data = request.get_json()
    required_fields = ['user_id', 'business_id', 'stars']
    if not all(fields in data for fields in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400
    
    business_key = datastore_client.key('Business', data['business_id'])
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    # Check if the user has already submitted a review for this business
    query = datastore_client.query(kind='Review')
    query.add_filter(filter=('user_id', '=', data['user_id']))
    query.add_filter(filter=('business_id', '=', data['business_id']))
    existing_reviews = list(query.fetch())
    if existing_reviews:
        return jsonify({'Error': 'You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review'}), 409
    
    # Add review to Datastore
    review_key = datastore_client.key('Review')
    review = datastore.Entity(key=review_key)
    review.update(data)
    datastore_client.put(review)

    # Add review ID to the response
    review['id'] = review.key.id

    return jsonify(review), 201

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)