# Run this script several times until the cluster status becomes Available,
# then copy the DWH_ENDPOINT and paste it into Admin > Connections > redshift > Host
# ---------------------------------------------------------------------

import pandas as pd
import json
from pprint import pprint

# Load DWH Params from a file
# ---------------------------------------------------------------------

import configparser
config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_NUM_NODES          = int(config.get("DWH","DWH_NUM_NODES"))
if DWH_NUM_NODES == 1:
	DWH_CLUSTER_TYPE = 'single-node'
else:
	DWH_CLUSTER_TYPE = 'multi-node'

DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_REGION             = config.get("DWH", "DWH_REGION")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

# Create clients for IAM, EC2, S3 and Redshift
# ---------------------------------------------------------------------

import boto3

redshift = boto3.client('redshift',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Describe cluster
# ---------------------------------------------------------------------

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
pprint(prettyRedshiftProps(myClusterProps))

# Print out Endpoint and role:
# ---------------------------------------------------------------------

DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)