import os
from . import app
from .cluster_nodes import boto3, botocore, ValidationError
from flask import Flask, jsonify, request
from .validrequest import S3bucket

client = boto3.client("s3")
s3bucket_requestschema = S3bucket()

@app.route("/api/raptor/s3bucket/create", methods=["POST"])
def create_bucket():
    request_payload = request.get_json()
    # request validation
    try:
        valid_data = s3bucket_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    bucketname = valid_data["bucketname"]
    location = valid_data["location"]

    response = client.create_bucket(
        ACL='public-read-write',
        Bucket=bucketname,
        CreateBucketConfiguration={
            'LocationConstraint': location
        }
    )
    return response