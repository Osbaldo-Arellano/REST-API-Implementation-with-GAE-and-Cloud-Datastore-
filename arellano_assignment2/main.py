from flask import Flask, jsonify, request
from google.cloud import datastore

app = Flask(__name__)
datastore_client = datastore.Client()

def format_business_response(business):
    return {
        'id': business.key.id,
        'owner_id': business['owner_id'],
        'name': business['name'],
        'street_address': business['street_address'],
        'city': business['city'],
        'state': business['state'],
        'zip_code': business['zip_code']
    }

def format_review_response(review):
    response = {
        'id': review.key.id,
        'user_id': review['user_id'],
        'business_id': review['business_id'],
        'stars': review['stars']
    }
    if 'review_text' in review:
        response['review_text'] = review['review_text']
    else:
        response['review_text'] = None
    return response

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

    return jsonify(format_business_response(business)), 201

@app.route('/businesses', methods=['GET'])
def get_businesses():
    query = datastore_client.query(kind='Business')
    businesses = list(query.fetch())

    # include the id in the response
    for business in businesses:
        business['id'] = business.key.id

    return jsonify([format_business_response(business) for business in businesses]), 200

@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business_by_id(business_id):
    business_key = datastore_client.key('Business', business_id)
    business = datastore_client.get(business_key)

    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404
    
    return jsonify(format_business_response(business)), 200

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
    
    return jsonify(format_business_response(business)), 200

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
    query.add_filter('owner_id', '=', owner_id)
    businesses = list(query.fetch())
    return jsonify([format_business_response(business) for business in businesses]), 200

@app.route('/reviews', methods=['POST'])
def create_review():
    data = request.get_json()
    required_fields = ['user_id', 'business_id', 'stars']
    if not all(field in data for field in required_fields):
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400
    
    business_key = datastore_client.key('Business', data['business_id'])
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({'Error': 'No business with this business_id exists'}), 404

    # Check if the user already reviewed this business
    query = datastore_client.query(kind='Review')
    query.add_filter('user_id', '=', data['user_id'])
    query.add_filter('business_id', '=', data['business_id'])
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


@app.route('/reviews/<review_id>', methods=['GET'])
def get_review(review_id):
    try:
        review_id = int(review_id)
    except ValueError:
        return jsonify({'Error': 'review_id must be an integer'}), 200

    review_key = datastore_client.key('Review', review_id)
    review = datastore_client.get(review_key)

    if not review:
        return jsonify({'Error': 'No review with this review_id exists'}), 404

    review['id'] = review_id
    return jsonify(review), 200

@app.route('/reviews/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    data = request.get_json()
    if 'stars' not in data:
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400
    
    review_key = datastore_client.key('Review', review_id)
    review = datastore_client.get(review_key)
    if not review:
        return jsonify({'Error': 'No review with this review_id exists'}), 404
    
    # Append 'stars' and/or 'review' to the response 
    if 'stars' in data:
        review['stars'] = data['stars']
    if 'review_text' in data:
        review['review_text'] = data['review_text']

    datastore_client.put(review)

    review['id'] = review_id

    return jsonify(review), 200


@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    review_key = datastore_client.key('Review', review_id)
    review = datastore_client.get(review_key)

    if not review:
        return jsonify({'Error': 'No review with this review_id exists'}), 404
    
    datastore_client.delete(review_key)

    return '', 204

@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_user_reviews(user_id):
    query = datastore_client.query(kind='Review')
    query.add_filter('user_id', '=', user_id)
    reviews = list(query.fetch())
    
    for review in reviews:
        review['id'] = review.key.id

    return jsonify(reviews), 200

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)