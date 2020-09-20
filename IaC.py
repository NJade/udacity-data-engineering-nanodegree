import pandas as pd
import boto3
import json
import configparser
import sys

def create_iam_role(iam, config):
    IAM_ROLE_NAME = config.get('IAM_ROLE', 'NAME')
    dwhRole = iam.create_role(
        Path = '/',
        RoleName = IAM_ROLE_NAME,
        AssumeRolePolicyDocument = json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
        )
    )
    
    POLICY_ARN = config.get("IAM_ROLE", "ARN")
    iam.attach_role_policy(
        RoleName = IAM_ROLE_NAME,
        PolicyArn = POLICY_ARN
    )
    return iam.get_role(RoleName = IAM_ROLE_NAME)['Role']['Arn']
    
def delete_iam_role(iam, config):
    IAM_ROLE_NAME = config.get('IAM_ROLE', 'NAME')
    POLICY_ARN = config.get("IAM_ROLE", "ARN")
    iam.detach_role_policy(RoleName = IAM_ROLE_NAME,
                          PolicyArn=POLICY_ARN)
    iam.delete_role(RoleName = IAM_ROLE_NAME)
    
def create_redshift_cluster(redshift, iam_role_arn, config):
    DWH_CLUSTER_TYPE = config.get("DWH", "CLUSTER_TYPE")
    NODE_TYPE = config.get("DWH", "NODE_TYPE")
    DWH_NUM_NODES = config.get("DWH", "NUM_NODES")
    DWH_DB_NAME = config.get("DWH", "DB_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    DWH_DB_USER = config.get("DWH", "DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DB_PASSWORD")
    
    redshift.create_cluster(
        ClusterType = DWH_CLUSTER_TYPE,
        NodeType= NODE_TYPE,
        NumberOfNodes = int(DWH_NUM_NODES),
        DBName = DWH_DB_NAME,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        IamRoles = [iam_role_arn]
    )

def delete_redshift_cluster(redshift, config):
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
def open_inbound_ip(ec2, redshift, config):
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    DWH_PORT = config.get("DWH", "PORT")
    
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    vpc = ec2.Vpc(id = cluster_props['VpcId'])
    sg = list(vpc.security_groups.all())[0]
    exist_permissions = sg.ip_permissions
    if exist_permissions:
        sg.revoke_ingress(IpPermissions=exist_permissions)
    sg.authorize_ingress(
        GroupName=sg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    
def get_cluster_info(redshift, config):
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "CLUSTER_IDENTIFIER")
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print("END_POINT: " + cluster_props['Endpoint']['Address'])
    print("DWH_ROLE_ARN: " +  cluster_props['IamRoles'][0]['IamRoleArn'])
    
def main(argv):
    config = configparser.ConfigParser()
    config.read_file(open('iac.cfg'))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                           )
    
    if argv == 'c':
        iam_role_arn = create_iam_role(iam, config)
        create_redshift_cluster(redshift, iam_role_arn, config)
        open_inbound_ip(ec2, redshift, config)
    elif argv == 'd':
        delete_redshift_cluster(redshift, config)
        delete_iam_role(iam, config)    
    elif argv == 'g':
        get_cluster_info(redshift, config)

if __name__ == "__main__":
    main(sys.argv[1])
    