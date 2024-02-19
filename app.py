from flask import Flask, request, Response
import logging
import pika
import json
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# Connect to MongoDB 
client = MongoClient('mongodb://mongodb:27017/')
db = client["apartment_db"]
collection = db["apartments"]

# Set up logging
logging.basicConfig(level=logging.INFO)

# Update the publish_apartment_event function
def publish_apartment_event(action, apartment_id):
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="apartments", exchange_type="fanout")
    channel.basic_publish(exchange="apartments", routing_key="", body=json.dumps({"id": apartment_id}))
    connection.close()


@app.route("/add")
def add():
    try:
        name = request.args.get("name")
        address = request.args.get("address")
        noiselevel = request.args.get("noiselevel")
        floor = request.args.get("floor")

        if name is None or address is None or noiselevel is None or floor is None:
            raise ValueError("Missing required parameters")

        noiselevel = float(noiselevel)
        floor = int(floor)

        # Check if apartment already exists
        existing_apartment = collection.find_one({"name": name, "address": address})
        if existing_apartment:
            return Response('{"result": false, "error": 2, "description": "Apartment already exists"}', status=400, mimetype="application/json")

        # Add apartment
        result = collection.insert_one({"name": name, "address": address, "noiselevel": noiselevel, "floor": floor})

        # Notify everybody that the apartment was added
        publish_apartment_event("added", str(result.inserted_id))

        return Response('{"result": true, "description": "Apartment was added successfully."}', status=201, mimetype="application/json")

    except ValueError as ve:
        app.logger.error(f"Error adding apartment: {ve}")
        return Response('{"result": false, "error": 400, "description": "Bad Request"}', status=400, mimetype="application/json")

    except Exception as e:
        app.logger.error(f"Error adding apartment: {e}")
        return Response('{"result": false, "error": 500, "description": "Internal Server Error"}', status=500, mimetype="application/json")


@app.route("/remove")
def remove():
    try:
        apartment_id = request.args.get("id")

        if apartment_id is None:
            raise ValueError("Missing ID parameter")

        # Check if apartment exists
        existing_apartment = collection.find_one({"_id": ObjectId(apartment_id)})
        if not existing_apartment:
            return Response('{"result": false, "error": 2, "description": "Apartment does not exist"}', status=400, mimetype="application/json")

        # Delete apartment
        collection.delete_one({"_id": ObjectId(apartment_id)})

        # Notify everybody that the apartment was DELETED
        publish_apartment_event("deleted", apartment_id)

        return Response('{"result": true, "description": "Apartment was deleted successfully."}', status=201, mimetype="application/json")

    except ValueError as ve:
        app.logger.error(f"Error removing apartment: {ve}")
        return Response('{"result": false, "error": 400, "description": "Bad Request"}', status=400, mimetype="application/json")

    except Exception as e:
        app.logger.error(f"Error removing apartment: {e}")
        return Response('{"result": false, "error": 500, "description": "Internal Server Error"}', status=500, mimetype="application/json")

@app.route("/list")
def list_apartments():
    try:
        apartments_data = list(collection.find({}, {"_id": 1, "name": 1, "address": 1, "noiselevel": 1, "floor": 1}))
        formatted_apartments = [{"id": str(apartment["_id"]), "name": apartment["name"], "address": apartment["address"],
                                 "noiselevel": apartment["noiselevel"], "floor": apartment["floor"]} for apartment in apartments_data]

        return json.dumps({"apartments": formatted_apartments})

    except Exception as e:
        app.logger.error(f"Error listing apartments: {e}")
        return Response('{"result": false, "error": 500, "description": "Internal Server Error"}', status=500, mimetype="application/json")

@app.route("/")
def hello():
    return "Hello World from apartments!"

if __name__ == "__main__":
    logging.info("Starting the web server.")
    app.run(host="0.0.0.0", threaded=True)
