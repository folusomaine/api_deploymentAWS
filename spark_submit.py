from .cluster_nodes import Flask, jsonify, request, boto3, ValidationError
import os
from . import app
from .validrequest import SparkSubmitRequest

client = boto3.client("eks")
linux_basepath = os.environ["HOME"]
sparksubmit_requestschema = SparkSubmitRequest()

@app.route("/api/spark/job", methods=["POST"])
def submit_job():
    request_payload = request.get_json()
    # request validation
    try:
        valid_data = sparksubmit_requestschema.load(request_payload)
    except ValidationError as e:
        return jsonify({"message": e.messages}), 400

    # collect endpoint for spark job
    response = client.describe_cluster(
        name=os.environ["clusterName"]
    )
    endpoint = response["cluster"]["endpoint"]
    ##########################################
    file_loc = valid_data["file_loc"]
    instances = valid_data["instances"]
    job_name = os.environ["clusterName"]
    dockerRepo = valid_data["dockerRepo"]

    # linux path to default spark submit file
    spark_submit_path = linux_basepath + "/dir/spark-3.0.1-bin-hadoop2.7/bin/spark-submit" 
    # windows local path to spark submit file 
    # spark_submit_path = os.path.join(r"C:\\Users\dir\spark-3.0.1-bin-hadoop2.7\bin\spark-submit")

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
    
    result = {"message": "%s job completed successfully!" %job_name}
    return jsonify(result)



