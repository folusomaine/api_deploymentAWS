from marshmallow import Schema, fields, validate

class EksClusterRequest(Schema):
    name = fields.Str(required=True)
    securityGroupIds = fields.Str(required=True)
    subnetId1 = fields.Str(required=True)
    subnetId2 = fields.Str(required=True)
    version = fields.Str(required=False, missing="")
    endpointPublicAccess = fields.Boolean(missing=True, required=False, validate=validate.OneOf([True, False]))
    endpointPrivateAccess = fields.Boolean(missing=False, required=False, validate=validate.OneOf([True, False]))
    publicAccessCidrs = fields.Str(missing="0.0.0.0/0", required=False)

class NodeGroupRequest(Schema):
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

class S3bucket(Schema):
    bucketname = fields.Str(required=True)
    location = fields.Str(required=True)
