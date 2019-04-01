1. Update `config_default.cfg` to include your AWS key and secret. The user associated with this key needs to have `AdministratorAccess` (because it needs to add a user role and create a RedShift cluster).
2. Go to Admin > Connections > Create a connection as follows:
  - Conn Id: 'aws_credentials'
  - Login: AWS key (does not have to be the same one with the one used in #1. It needs to have `S3ReadOnlyAccess`)
  - Password: AWS Secret key 
3. Go to Admin > Variables > Create a variable `append_only` and set its value to `0`. When set to `1` it will not attempt to recreate dimensions tables (Fact tables are always recreated).
4. Run `setup.py` to create an Amazon RedShift cluster from the configuration given in `config.cfg`.
5. You may revoke the administrator access from the user in #1 above. Make sure to add `AmazonRedshiftFullAccess` and `AmazonS3ReadOnlyAccess` so we may access the RedShift cluster and S3 storage with it.
6. Run `status.py` to check if the cluster has been created. Once created, you should see values for DWH_ENDPOINT and DWH_ROLE_ARN.
7. Go to Admin > Connections > Create with the following details:
  - Conn Id: 'redshift'
  - Conn Type: 'PostgreSQL'
  - Host: Paste the DWH_ENDPOINT to this field.
  - Login: Paste the value of DWH_DB_USER from `config.cfg` to this field.
  - Password: Paste the value of DWH_DB_PASSWORD from `config.cfg` to this field.
  - Port: Paste the value of DWH_PORT from `config.cfg` to this field.
8. Go to Admin > Variables > Create a variable `redshift_iam_role` with value from DWH_ROLE_ARN. (We use this instead of AWS credentials because this IAM role has less access than )
9. After completing all tests, remove the RedShift cluster so it does not incur any cost by running `remove.py` script.

Checking Queries (run the following from RedShift's Query Editor):

```
select * from pg_catalog."stl_load_errors"
where filename like '%song%'
order by starttime desc
limit 10;
```