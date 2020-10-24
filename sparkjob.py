from .ekscluster import boto3
from .ekscluster import Flask, jsonify, request
import os
from . import app

client = boto3.client("eks")
os_basepath = os.environ["HOME"]

@app.route("/api/spark/kub/job", methods=["POST"])
def submit_job():
    # collect parameters for spark job
    response = client.describe_cluster(
        name=os.environ["clusterName"]
    )
    endpoint = response["cluster"]["endpoint"]
    file_loc = request.get_json()["file_loc"]
    instances = request.get_json()["instances"]
    job_name = request.get_json()["job_name"]
    dockerRepo = request.get_json()["dockerRepo"]
    spark_submit_path = os_basepath + "/Documents/spark-3.0.1-bin-hadoop2.7/bin/spark-submit"

    ## resolve dependencies
    clusterName = os.environ['clusterName']
    regionCode = os.environ['regionCode']
    # set credentials in ~/.kube/config
    cmd1 = f'aws eks --region {regionCode} update-kubeconfig --name {clusterName}'
    # enable k8s service account role for spark job
    cmd2 = f'kubectl create clusterrolebinding {clusterName} --clusterrole cluster-admin --serviceaccount=default:default'
    os.system(cmd1)
    os.system(cmd2)

    # submit job command
    cmd = f"{spark_submit_path} \
                --master k8s://{endpoint}:443 \
                --deploy-mode cluster \
                --name {job_name} \
                --class org.apache.spark.examples.SparkPi \
                --conf spark.executor.instances={instances} \
                --conf spark.kubernetes.container.image={dockerRepo}/spark:latest \
                {file_loc}"
    
    # execute command
    os.system(cmd)
    # logging.info("Running spark job %s" % job_name)
    result = {"message": "%s job completed successfully!" %job_name}
    return jsonify(result)




