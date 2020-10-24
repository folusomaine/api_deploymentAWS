import os
import boto3, botocore
from app import app
from app.projectapi import Flask, jsonify, request

client = boto3.client("s3")

@app.route("/api/raptor/s3bucket/create", methods=["POST"])
def create_bucket():
    bucketname = request.get_json()["bucketname"]
    location = request.get_json()["location"]

    response = client.create_bucket(
        ACL='public-read-write',
        Bucket=bucketname,
        CreateBucketConfiguration={
            'LocationConstraint': location
        }
    )
    return response