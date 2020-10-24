import os
import boto3, botocore
from app import app
from app.projectapi import Flask, jsonify, request
from marshmallow import ValidationError
from app.spark_clusters.validrequest import EksClusterRequest, NodeGroupRequest

eks_requestschema = EksClusterRequest()
nodegroup_requestschema = NodeGroupRequest()
client = boto3.client("eks")

#create eks cluster
@app.route("/api/spark/kub/cluster", methods=["POST"])
def create_kub_cluster():
    request_payload = request.get_json()
    try:
        valid_data = eks_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    name = valid_data["name"]
    securityGroupIds = valid_data["securityGroupIds"]
    subnetId1 = valid_data["subnetId1"]
    subnetId2 = valid_data["subnetId2"]
    roleArn = valid_data["roleArn"]
    region_code = valid_data["region_code"]
    endpointPrivateAccess = valid_data["endpointPrivateAccess"]
    endpointPublicAccess = valid_data["endpointPublicAccess"]
    publicAccessCidrs = valid_data["publicAccessCidrs"]

    # store cluster name and region code as env var
    os.environ["clusterName"] = name
    os.environ["regionCode"] = region_code
    
    try:
        response = client.create_cluster(
            name=name,
            resourcesVpcConfig={
                "securityGroupIds": [
                    securityGroupIds,
                ],
                "subnetIds": [
                    subnetId1,
                    subnetId2,
                ],
                "endpointPublicAccess": endpointPublicAccess,
                "endpointPrivateAccess": endpointPrivateAccess,
                'publicAccessCidrs': [
                    publicAccessCidrs,
                ]
            },
            roleArn=roleArn,
        )
        result = {
            "message": "success",
            "status": 200,
            "data": response
            }
    except botocore.exceptions.ClientError as e:
        result = {
            "message": "invalid request",
            "status": 400,
            "data": e.response
            }
    return jsonify(result)

# deploy node group for created eks cluster
@app.route("/api/spark/kub/nodegroup", methods=["POST"])
def create_kub_nodegroup():
    request_payload = request.get_json()
    try:
        valid_data = nodegroup_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    nodegroupName = valid_data["nodegroupName"]
    minSize = valid_data["minSize"]
    maxSize = valid_data["maxSize"]
    desiredSize = valid_data["desiredSize"]
    diskSize = valid_data["diskSize"]
    instanceTypes = valid_data["instanceTypes"]
    amiType = valid_data["amiType"]
    subnetId1 = valid_data["subnetId1"]
    subnetId2 = valid_data["subnetId2"]
    nodeRole = valid_data["nodeRole"]

    # store nodegroupName as env var
    os.environ["nodegroupName"] = nodegroupName

    try:
        response = client.create_nodegroup(
            clusterName=os.environ["clusterName"],
            nodegroupName=nodegroupName,
            scalingConfig={
                'minSize': minSize,
                'maxSize': maxSize,
                'desiredSize': desiredSize
            },
            diskSize=diskSize,
            subnets=[
                subnetId1,
                subnetId2,
            ],
            instanceTypes=[
                instanceTypes,
            ],
            amiType=amiType, #default is "AL2_x86_64" | for GPU instanceType select "AL2_x86_64_GPU"
            tags={
                "kubernetes.io/cluster/%s" % os.environ["clusterName"]: "owned"
            },
            nodeRole=nodeRole
        )
        result = {
            "message": "success",
            "status": 200,
            "data": response
            }
    except botocore.exceptions.ClientError as e:
        result = {
            "message": "invalid request",
            "status": 400,
            "data": e.response
            }
    return jsonify(result)

# check cluster status 
@app.route("/api/spark/kub/cluster/status", methods=["GET"])
def cluster_status():
    name = os.environ["clusterName"]
    try:
        response = client.describe_cluster(
            name=name
        )
        # filter response
        name = response["cluster"]["name"]
        createdAt = response["cluster"]["createdAt"]
        version = response["cluster"]["version"]
        status = response["cluster"]["status"]
        result = {"message": {
            "name": name,
            "createdAt": createdAt,
            "version": version,
            "status": status
        }}
        return jsonify(result)
    except botocore.exceptions.ClientError as e:
        return jsonify({"message": e.response}), 404

# delete cluster
@app.route("/api/spark/kub/cluster/delete", methods=["DELETE"])
def cluster_delete():
    name = os.environ["clusterName"]
    try:
        response = client.delete_cluster(
            name=name
        )
        # filter response
        name = response["cluster"]["name"]
        createdAt = response["cluster"]["createdAt"]
        version = response["cluster"]["version"]
        status = response["cluster"]["status"]
        result = {"message": {
            "name": name,
            "createdAt": createdAt,
            "version": version,
            "status": status
        }}
        return jsonify(result)
    except botocore.exceptions.ClientError as e:
        return jsonify({"message": e.response}), 404

# delete nodegroup
@app.route("/api/spark/kub/nodegroup/delete", methods=["DELETE"])
def nodegroup_delete():
    clusterName = os.environ["clusterName"]
    nodegroupName = os.environ["nodegroupName"]
    try:
        response = client.delete_nodegroup(
            clusterName=clusterName,
            nodegroupName=nodegroupName
        )
        # filter response
        nodegroupName = response["nodegroup"]["nodegroupName"]
        clusterName = response["nodegroup"]["clusterName"]
        status = response["nodegroup"]["status"]
        health = response["nodegroup"]["health"]
        result = {"message": {
            "nodegroupName": nodegroupName,
            "clusterName": clusterName,
            "status": status,
            "health": health
        }}
        return jsonify(result)
    except botocore.exceptions.ClientError as e:
        return jsonify(e.response), 404
    
# check nodegroup status
@app.route("/api/spark/kub/nodegroup/status", methods=["GET"])
def nodegroup_status():
    clusterName = os.environ["clusterName"]
    nodegroupName = os.environ["nodegroupName"]
    try:
        response = client.describe_nodegroup(
            clusterName=clusterName,
            nodegroupName=nodegroupName
        )
        # filter response
        nodegroupName = response["nodegroup"]["nodegroupName"]
        clusterName = response["nodegroup"]["clusterName"]
        status = response["nodegroup"]["status"]
        health = response["nodegroup"]["health"]
        result = {"message": {
            "nodegroupName": nodegroupName,
            "clusterName": clusterName,
            "status": status,
            "health": health
        }}
        return jsonify(result)
    except botocore.exceptions.ClientError as e:
        return jsonify(e.response), 404