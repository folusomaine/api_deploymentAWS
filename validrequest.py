from marshmallow import Schema, fields, validate

class EksClusterRequest(Schema):
    name = fields.Str(required=True)
    securityGroupIds = fields.Str(required=True)
    subnetId1 = fields.Str(required=True)
    subnetId2 = fields.Str(required=True)
    roleArn = fields.Str(required=True)
    region_code = fields.Str(required=True)
    endpointPublicAccess = fields.Boolean(missing=True, required=False, validate=validate.OneOf([True, False]))
    endpointPrivateAccess = fields.Boolean(missing=False, required=False, validate=validate.OneOf([True, False]))
    publicAccessCidrs = fields.Str(missing="0.0.0.0/0", required=False)

class NodeGroupRequest(Schema):
    nodegroupName = fields.Str(required=True)
    subnetId1 = fields.Str(required=True)
    subnetId2 = fields.Str(required=True)
    nodeRole = fields.Str(required=True)
    minSize = fields.Int(missing=1, required=False)
    maxSize = fields.Int(missing=2, required=False)
    desiredSize = fields.Int(missing=2, required=False)
    diskSize = fields.Int(missing=20, required=False)
    instanceTypes = fields.Str(missing="t3.medium", required=False)
    amiType = fields.Str(missing="AL2_x86_64", required=False, validate=validate.OneOf(["AL2_x86_64", "AL2_x86_64_GPU"]))

class SparkSubmitRequest(Schema):
    file_loc = fields.Str(required=True)
    dockerRepo = fields.Str(required=True)
    instances = fields.Str(required=False, missing=5)
    job_name = fields.Str(required=False, missing="spark-pi")
    
