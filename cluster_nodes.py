import os
import boto3, botocore
from . import app, spark_submit, create_bucket # entrypoint for flask run
from flask import Flask, jsonify, request
from marshmallow import ValidationError
from .validrequest import EksClusterRequest, NodeGroupRequest

eks_requestschema = EksClusterRequest()
nodegroup_requestschema = NodeGroupRequest()
client = boto3.client("eks")
iam_client = boto3.client('iam')

#create eks cluster
@app.route("/api/kub/cluster/create", methods=["POST"])
def create_kub_cluster():
    request_payload = request.get_json()
    # request validation
    try:
        valid_data = eks_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    name = valid_data["name"]
    securityGroupIds = valid_data["securityGroupIds"]
    subnetId1 = valid_data["subnetId1"]
    subnetId2 = valid_data["subnetId2"]
    version = valid_data["version"]
    endpointPrivateAccess = valid_data["endpointPrivateAccess"]
    endpointPublicAccess = valid_data["endpointPublicAccess"]
    publicAccessCidrs = valid_data["publicAccessCidrs"]

    ############################################################
    ## eks cluster dependency
    # create iam role to deploy cluster
    create_iam = iam_client.create_role(
        AssumeRolePolicyDocument='{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "eks.amazonaws.com"}, "Action": "sts:AssumeRole"}]}',
        Path='/',
        RoleName=f'{name}_eksclusterRole',
        Tags=[
            {
                'Key': f'kubernetes.io/cluster/{name}',
                'Value': 'owned'
            },
        ]
    )
    
    # attach eks service policy to (created) role
    iam_client.attach_role_policy(
        RoleName=f'{name}_eksclusterRole',
        PolicyArn='arn:aws:iam::aws:policy/AmazonEKSClusterPolicy' # default eks service arn
    )

    arn = create_iam["Role"]["Arn"]
    #################################################################

    # store params as env var
    os.environ["clusterName"] = name
    os.environ["subnetId1"] = subnetId1
    os.environ["subnetId2"] = subnetId2
    
    try:
        response = client.create_cluster(
            name=name,
            version=str(version),
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
            roleArn=arn,
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

# deploy nodegroup for created eks cluster
@app.route("/api/spark/kub/nodegroup/create", methods=["POST"])
def create_kub_nodegroup():
    request_payload = request.get_json()
    try:
        valid_data = nodegroup_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    clusterName = os.environ["clusterName"]
    nodegroupName = f"{clusterName}_nodegroup"
    minSize = valid_data["minSize"]
    maxSize = valid_data["maxSize"]
    desiredSize = valid_data["desiredSize"]
    diskSize = valid_data["diskSize"]
    instanceTypes = valid_data["instanceTypes"]
    amiType = valid_data["amiType"]
    subnetId1 = os.environ["subnetId1"]
    subnetId2 = os.environ["subnetId2"]

    # store nodegroupName as env var
    os.environ["nodegroupName"] = nodegroupName

    ############################################################
    ## nodegroup deployment dependency
    # create iam role to deploy nodegroup
    create_iam = iam_client.create_role(
        AssumeRolePolicyDocument='{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}',
        Path='/',
        RoleName=f'{nodegroupName}_role',
        Tags=[
            {
                'Key': f'kubernetes.io/{os.environ["clusterName"]}_cluster/{nodegroupName}',
                'Value': 'owned'
            },
        ]
    )
    
    # attach required policies to (created) role
    iam_client.attach_role_policy(
        RoleName=f'{nodegroupName}_role',
        PolicyArn='arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy'
    )
    iam_client.attach_role_policy(
        RoleName=f'{nodegroupName}_role',
        PolicyArn='arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly'
    )
    iam_client.attach_role_policy(
        RoleName=f'{nodegroupName}_role',
        PolicyArn='arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy'
    )

    arn = create_iam["Role"]["Arn"]
    #################################################################
    
    ########################################
    ## resolve spark-submit job dependencies
    clusterName = os.environ['clusterName']
    regionCode = os.environ['AWS_DEFAULT_REGION']
    # set credentials in ~/.kube/config to enable kubectl using AWS CLI
    cmd1 = f'aws eks --region {regionCode} update-kubeconfig --name {clusterName}'
    # enable k8s service account role for spark job using KUBECTL
    cmd2 = f'kubectl create clusterrolebinding {clusterName} --clusterrole cluster-admin --serviceaccount=default:default'
    os.system(cmd1) #run command
    os.system(cmd2) #run command
    ##########################################

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
            nodeRole=arn
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


###########################################
# infrastructure statuses and clean up
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
        # detach policy first
        iam_client.detach_role_policy(
            RoleName=f"{name}_eksclusterRole",
            PolicyArn="arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
        )

        # delete associated role
        iam_client.delete_role(
            RoleName=f"{name}_eksclusterRole"
        )

        # delete cluster endpoint
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
        ### must remove roles from instance profile first
        # get instanceProfileName first from associated iam role
        getInstanceProfile = iam_client.list_instance_profiles_for_role(
            RoleName=f'{nodegroupName}_role'
        )
        instanceProfileName = getInstanceProfile["InstanceProfiles"][0]['InstanceProfileName']

        # remove role from instance profile with the retrieved instanceProfileName
        iam_client.remove_role_from_instance_profile(
            InstanceProfileName=instanceProfileName,
            RoleName=f'{nodegroupName}_role'
        )

        # then detach policies from associated iam role
        iam_client.detach_role_policy(
            RoleName=f'{nodegroupName}_role',
            PolicyArn="arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
        )
        iam_client.detach_role_policy(
            RoleName=f'{nodegroupName}_role',
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
        )
        iam_client.detach_role_policy(
            RoleName=f'{nodegroupName}_role',
            PolicyArn="arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
        )

        # then delete associated role
        iam_client.delete_role(
            RoleName=f'{nodegroupName}_role'
        )

        # delete nodes
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