/*
Lesson 1: one-time Snowflake bootstrap. Run in a Snowsight worksheet as a user that holds
ACCOUNTADMIN (which inherits SYSADMIN and SECURITYADMIN).

Creates:
  MEDALLION_WH    X-Small warehouse, suspends after 60 s idle
  MEDALLION_DB    BRONZE (STG), SILVER (TMP), GOLD (TGT + Aggregate), AUDIT schemas
                  (owned by MEDALLION_ROLE)
  MEDALLION_ROLE  everything the pipeline needs, nothing more
  AIRFLOW_SVC     service user, key-pair auth only (no password)

Safe to re-run: every statement is IF NOT EXISTS or an idempotent GRANT.
Before running, replace <PASTE_PUBLIC_KEY_BODY> with the body of secrets/rsa_key.pub
(the lines between BEGIN/END PUBLIC KEY, joined into one line).
*/

-- Quoted so IDENTIFIER() matches names containing dots, @ or lowercase letters exactly.
SET my_user = '"' || CURRENT_USER() || '"';

-- Objects are owned by SYSADMIN; roles, users and grants are managed by SECURITYADMIN.
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS MEDALLION_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Airflow medallion pipeline';

CREATE DATABASE IF NOT EXISTS MEDALLION_DB
    COMMENT = 'Airflow medallion pipeline: BRONZE, SILVER, GOLD, AUDIT';

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS MEDALLION_ROLE
    COMMENT = 'Owns pipeline schemas; used by AIRFLOW_SVC';

-- Custom roles should roll up to SYSADMIN so admins can still see their objects.
GRANT ROLE MEDALLION_ROLE TO ROLE SYSADMIN;
GRANT ROLE MEDALLION_ROLE TO USER IDENTIFIER($my_user);

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE MEDALLION_WH TO ROLE MEDALLION_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE MEDALLION_DB TO ROLE MEDALLION_ROLE;

-- Creating the schemas as MEDALLION_ROLE makes it their owner, so the pipeline can
-- create tables, stages, sequences and file formats inside them.
USE ROLE MEDALLION_ROLE;
USE WAREHOUSE MEDALLION_WH;

CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.BRONZE COMMENT = 'STG tables: raw landing, no transformation';
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.SILVER COMMENT = 'TMP tables: clean and compare against target';
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.GOLD   COMMENT = 'TGT (DIM_, FCT_) and Aggregate (AGG_, VW_)';
CREATE SCHEMA IF NOT EXISTS MEDALLION_DB.AUDIT  COMMENT = 'Control tables (C_), rejected rows and reconciliation';

USE ROLE SECURITYADMIN;

-- TYPE = SERVICE: cannot log in with a password or to Snowsight, so it is exempt from MFA
-- and can only authenticate with the key pair below.
CREATE USER IF NOT EXISTS AIRFLOW_SVC
    TYPE              = SERVICE
    DEFAULT_ROLE      = MEDALLION_ROLE
    DEFAULT_WAREHOUSE = MEDALLION_WH
    DEFAULT_NAMESPACE = MEDALLION_DB
    RSA_PUBLIC_KEY    = '<PASTE_PUBLIC_KEY_BODY>'
    COMMENT           = 'Airflow medallion pipeline';

-- Re-running with a new key: this updates the key on an existing user.
ALTER USER AIRFLOW_SVC SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_BODY>';

GRANT ROLE MEDALLION_ROLE TO USER AIRFLOW_SVC;

-- Verify. DESC USER needs a role with privileges on the user; MEDALLION_ROLE has none.
DESC USER AIRFLOW_SVC;  -- RSA_PUBLIC_KEY_FP should start with SHA256:

USE ROLE MEDALLION_ROLE;
SHOW SCHEMAS IN DATABASE MEDALLION_DB;
