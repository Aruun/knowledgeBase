import sys
import json
from urllib.parse import urlparse
import urllib
import datetime
import boto3
import botocore
import time
import logging
import configparser
import os
import subprocess
import time
from configparser import NoOptionError, NoSectionError

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from botocore.exceptions import ClientError

glue = boto3.client('glue', region_name='us-east-1')

if len(sys.argv) < 2:
    logger.info(
        "Not enough arguments. Try again. Please ensure that conf file is added as argumnet : python3 glue_deploy.py glue_deployment.conf")
    sys.exit()

# Arguments passed
logger.info(f"Name of Python script: {sys.argv[0]}")
logger.info(f"Name of Conf File: {sys.argv[1]}")

# Initialise s3 Resource
s3 = boto3.resource('s3')

# Reading Config File
config = configparser.ConfigParser()
config.optionxform = str

if sys.argv[1][:2] == 's3':
    logger.info("Reading Conf file from s3 Path {}".format(sys.argv[1]))
    s3_uri = sys.argv[1].split("//")[1]
    bucket = s3_uri.split('/')[0]
    file_key = '/'.join([str(elem) for elem in s3_uri.split('/')[1:]])
    local_file = file_key.split('/')[-1]
    try:
        s3.Bucket(bucket).download_file(file_key, local_file)
        logger.info("Sucessfully Downloaded Conf File to local file {}".format(local_file))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The Conf File does not exist.")
        else:
            raise
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(local_file)
    os.remove(local_file)
else:
    config.read(sys.argv[1])

logger.info(f" config.sections {config.sections()}")


def check_glue_job_exists(job_name):
    """ Check if jlue Job job_name already exists.
    Args:
        job_name (string): Name of glue job to check if exists.
    Returns:
        (boolean)
        True if job already exists.
        False otherwise.
    """
    try:
        glue.get_job(JobName=job_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        False


def create_glue_job(job_params, opt_params, glue_dpus):
    """ Create Glue job with input configuration.
    Args:
        glueJobName (string): Name of glue job to create.
        glueScriptLocation (string): The S3 location where your
                                  ETL script is located
                                  (in a form like s3://path/to/my/script.scala)
        glueExtraJars (string): S3 path(s) to additional Java .jar file(s)
                             that AWS Glue will add to the Java classpath
                             before executing your script. Multiple values
                             must be complete paths separated by a comma (,).
        glueExecutionRole (string): IAM Role with permissions to create update glue jobs.
    Returns:
        (None)
    """
    if job_params['runtime-type'] == 'glueetl':
        logger.info("Creating Glue Spark ETL Job")
        if 'connection' in job_params:
            logger.info("Creating Glue Job with Connection")
            if 'WorkerType' in glue_dpus:
                logger.info("Creating Glue Job with WorkerType as {}".format(glue_dpus['WorkerType']))
                response = glue.create_job(
                    Name=job_params['glueJobName'],
                    Role=job_params['glueExecutionRole'],
                    ExecutionProperty={
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    Command={
                        'Name': job_params['runtime-type'],
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    DefaultArguments=opt_params,
                    GlueVersion='2.0',
                    WorkerType=glue_dpus['WorkerType'],
                    NumberOfWorkers=int(glue_dpus['NumberOfWorkers']),
                    Tags={
                        'access-org': 'edo',
                        'access-department': 'dps',
                        'access-team': 'mt'
                    },
                    Connections={'Connections': [job_params['connection']]}
                )
            else:
                logger.info(
                    "Creating Glue Job with Standard Worker type & MaxCapacity as {}".format(glue_dpus['MaxCapacity']))
                response = glue.create_job(
                    Name=job_params['glueJobName'],
                    Role=job_params['glueExecutionRole'],
                    ExecutionProperty={
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    Command={
                        'Name': job_params['runtime-type'],
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    DefaultArguments=opt_params,
                    GlueVersion='2.0',
                    MaxCapacity=glue_dpus['MaxCapacity'],
                    Tags={
                        'access-org': 'edo',
                        'access-department': 'dps',
                        'access-team': 'mt'
                    },
                    Connections={'Connections': [job_params['connection']]}
                )
            logger.info("Job Creation Started {}".format(response))
        else:
            if 'WorkerType' in glue_dpus:
                logger.info("Creating Glue Job with WorkerType as {}".format(glue_dpus['WorkerType']))
                response = glue.create_job(
                    Name=job_params['glueJobName'],
                    Role=job_params['glueExecutionRole'],
                    ExecutionProperty={
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    Command={
                        'Name': job_params['runtime-type'],
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    DefaultArguments=opt_params,
                    GlueVersion='2.0',
                    WorkerType=glue_dpus['WorkerType'],
                    NumberOfWorkers=int(glue_dpus['NumberOfWorkers']),
                    Tags={
                        'access-org': 'edo',
                        'access-department': 'dps',
                        'access-team': 'mt'
                    })
            else:
                logger.info(
                    "Creating Glue Job with Standard Worker type & MaxCapacity as {}".format(glue_dpus['MaxCapacity']))

                response = glue.create_job(
                    Name=job_params['glueJobName'],
                    Role=job_params['glueExecutionRole'],
                    ExecutionProperty={
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    Command={
                        'Name': job_params['runtime-type'],
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    DefaultArguments=opt_params,
                    GlueVersion='2.0',
                    MaxCapacity=glue_dpus['MaxCapacity'],
                    Tags={
                        'access-org': 'edo',
                        'access-department': 'dps',
                        'access-team': 'mt'
                    }
                )
            logger.info("Job Creation Started {}".format(response))

    elif job_params['runtime-type'] == 'pythonshell':
        logger.info("Creating Glue PythonShell Job")
        if 'connection' in job_params:
            logger.info("Creating Glue Job with Connection")
            response = glue.create_job(
                Name=job_params['glueJobName'],
                Role=job_params['glueExecutionRole'],
                ExecutionProperty={
                    'MaxConcurrentRuns': int(job_params['concurrency'])
                },
                Command={
                    'Name': 'pythonshell',
                    'ScriptLocation': job_params['glueScriptLocation'],
                    'PythonVersion': '3'
                },
                DefaultArguments=opt_params,
                GlueVersion='1.0',
                MaxCapacity=1,
                Tags={
                    'access-org': 'edo',
                    'access-department': 'dps',
                    'access-team': 'mt'
                },
                Connections={'Connections': [job_params['connection']]}
            )
        else:
            response = glue.create_job(
                Name=job_params['glueJobName'],
                Role=job_params['glueExecutionRole'],
                ExecutionProperty={
                    'MaxConcurrentRuns': int(job_params['concurrency'])
                },
                Command={
                    'Name': 'pythonshell',
                    'ScriptLocation': job_params['glueScriptLocation'],
                    'PythonVersion': '3'
                },
                DefaultArguments=opt_params,
                GlueVersion='1.0',
                MaxCapacity=1,
                Tags={
                    'access-org': 'edo',
                    'access-department': 'dps',
                    'access-team': 'mt'
                }
            )
        logger.info("Job Creation Started {}".format(response))


def update_glue_job(job_params, opt_params, glue_dpus):
    """ Update Glue job with input configuration.
        job_name (string): Name of glue job to create.
        script_location (string): The S3 location where your
                                  ETL script is located
                                  (in a form like s3://path/to/my/script.scala)
        extra_jars (string): S3 path(s) to additional Java .jar file(s)
                             that AWS Glue will add to the Java classpath
                             before executing your script. Multiple values
                             must be complete paths separated by a comma (,).
        job_role (string): IAM Role with permissions to create update glue jobs.
    Returns:
        (None)
    """
    if job_params['runtime-type'] == 'glueetl':
        logger.info("Updating Glue Spark ETL Job")
        if 'connection' in job_params:
            logger.info("Updating Glue Job with Connection")
            if 'WorkerType' in glue_dpus:
                logger.info("Updating Glue Job with WorkerType as {}".format(glue_dpus['WorkerType']))
                response = glue.update_job(
                    JobName=job_params['glueJobName'],
                    JobUpdate={
                        'Role': job_params['glueExecutionRole'],
                        'ExecutionProperty': {
                            'MaxConcurrentRuns': int(job_params['concurrency'])
                        },
                        'Command': {
                            'Name': job_params['runtime-type'],
                            'ScriptLocation': job_params['glueScriptLocation'],
                            'PythonVersion': '3'
                        },
                        'DefaultArguments': opt_params,
                        'GlueVersion': '2.0',
                        'WorkerType': glue_dpus['WorkerType'],
                        'NumberOfWorkers': int(glue_dpus['NumberOfWorkers']),
                        'Connections': {'Connections': [job_params['connection']]}
                    }
                )
            else:
                logger.info(
                    "Updating Glue Job with Standard Worker type & MaxCapacity as {}".format(glue_dpus['MaxCapacity']))
                response = glue.update_job(
                    JobName=job_params['glueJobName'],
                    JobUpdate={
                        'Role': job_params['glueExecutionRole'],
                        'ExecutionProperty': {
                            'MaxConcurrentRuns': int(job_params['concurrency'])
                        },
                        'Command': {
                            'Name': job_params['runtime-type'],
                            'ScriptLocation': job_params['glueScriptLocation'],
                            'PythonVersion': '3'
                        },
                        'DefaultArguments': opt_params,
                        'GlueVersion': '2.0',
                        'MaxCapacity': glue_dpus['MaxCapacity'],
                        'Connections': {'Connections': [job_params['connection']]}
                    }
                )
            logger.info("Job Updation Started {}".format(response))
        else:
            if 'WorkerType' in glue_dpus:
                logger.info("Updating Glue Job with WorkerType as {}".format(glue_dpus['WorkerType']))
                response = glue.update_job(
                    JobName=job_params['glueJobName'],
                    JobUpdate={
                        'Role': job_params['glueExecutionRole'],
                        'ExecutionProperty': {
                            'MaxConcurrentRuns': int(job_params['concurrency'])
                        },
                        'Command': {
                            'Name': job_params['runtime-type'],
                            'ScriptLocation': job_params['glueScriptLocation'],
                            'PythonVersion': '3'
                        },
                        'DefaultArguments': opt_params,
                        'GlueVersion': '2.0',
                        'WorkerType': glue_dpus['WorkerType'],
                        'NumberOfWorkers': int(glue_dpus['NumberOfWorkers'])
                    }
                )
            else:
                logger.info(
                    "Updating Glue Job with Standard Worker type & MaxCapacity as {}".format(glue_dpus['MaxCapacity']))
                response = glue.update_job(
                    JobName=job_params['glueJobName'],
                    JobUpdate={
                        'Role': job_params['glueExecutionRole'],
                        'ExecutionProperty': {
                            'MaxConcurrentRuns': int(job_params['concurrency'])
                        },
                        'Command': {
                            'Name': job_params['runtime-type'],
                            'ScriptLocation': job_params['glueScriptLocation'],
                            'PythonVersion': '3'
                        },
                        'DefaultArguments': opt_params,
                        'GlueVersion': '2.0',
                        'MaxCapacity': glue_dpus['MaxCapacity']}
                )
            logger.info("Job Updation Started {}".format(response))
    elif job_params['runtime-type'] == 'pythonshell':
        logger.info("Updating Glue PythonShell Job")
        if 'connection' in job_params:
            logger.info("Updating Glue Job with Connection")
            glue.update_job(
                JobName=job_params['glueJobName'],
                JobUpdate={
                    'Role': job_params['glueExecutionRole'],
                    'ExecutionProperty': {
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    'Command': {
                        'Name': 'pythonshell',
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    'DefaultArguments': opt_params,
                    'GlueVersion': '1.0',
                    'MaxCapacity': 1,
                    'Connections': {'Connections': [job_params['connection']]}
                }
            )
        else:
            glue.update_job(
                JobName=job_params['glueJobName'],
                JobUpdate={
                    'Role': job_params['glueExecutionRole'],
                    'ExecutionProperty': {
                        'MaxConcurrentRuns': int(job_params['concurrency'])
                    },
                    'Command': {
                        'Name': 'pythonshell',
                        'ScriptLocation': job_params['glueScriptLocation'],
                        'PythonVersion': '3'
                    },
                    'DefaultArguments': opt_params,
                    'GlueVersion': '1.0',
                    'MaxCapacity': 1,
                }
            )


def glue_job_deployment(glue_parameters, glue_script_params, glue_dpus):
    job_name = glue_parameters['glueJobName']

    if check_glue_job_exists(job_name):
        logger.info("Updating glue job: {}".format(job_name))
        update_glue_job(glue_parameters, glue_script_params, glue_dpus)
        time.sleep(5)
    else:
        logger.info("Creating glue job: {}".format(job_name))
        create_glue_job(glue_parameters, glue_script_params, glue_dpus)
        time.sleep(5)

    logger.info("Finished deploying Glue job.")


def glue_job_status(job_name, run_id):
    '''
    Return the Job Status of the Glue Job Run

    Args:
        job_name (str): Name of the AWS Glue Job
        run_id (str): Id of the AWS Glue Job run

    Return:
        string
    '''
    client = boto3.client('glue')

    response = client.get_job_run(
        JobName=job_name,
        RunId=run_id)

    try:
        response = client.get_job_run(JobName=job_name, RunId=run_id)
        status = response['JobRun']['JobRunState']
        logger.info(status)
    except ClientError as err:
        if err.response['Error']['Code'] == 'InvalidInputException':
            logger.error("Invalid Input")
        elif err.response['Error']['Code'] == 'EntityNotFoundException':
            logger.error("Not Found")
        elif err.response['Error']['Code'] == 'InternalServiceException':
            logger.error("Internal Service Exception")
        elif err.response['Error']['Code'] == 'OperationTimeoutException':
            logger.error("Operation Timeout Exception")
        else:
            logger.error("Unexpected error: %s", err)

    return status


def glue_job_exec_time(job_name, run_id):
    '''
    Return the Execution Time of the Glue Job Run

    Args:
        job_name (str): Name of the AWS Glue Job
        run_id (str): Id of the AWS Glue Job run

    Return:
        str
    '''
    client = boto3.client('glue')

    response = client.get_job_run(
        JobName=job_name,
        RunId=run_id)

    try:
        response = client.get_job_run(JobName=job_name, RunId=run_id)
        exec_time = response['JobRun']['ExecutionTime']
        logger.info(exec_time)
    except ClientError as err:
        if err.response['Error']['Code'] == 'InvalidInputException':
            logger.error("Invalid Input")
        elif err.response['Error']['Code'] == 'EntityNotFoundException':
            logger.error("Not Found")
        elif err.response['Error']['Code'] == 'InternalServiceException':
            logger.error("Internal Service Exception")
        elif err.response['Error']['Code'] == 'OperationTimeoutException':
            logger.error("Operation Timeout Exception")
        else:
            logger.error("Unexpected error: %s", err)

    return exec_time


def extract_zip(job_name, script_location, env):
    import boto3
    import zipfile
    from io import BytesIO
    s3 = boto3.client('s3', use_ssl=False)
    Key_unzip = 'glue/tempScript/{}/'.format(job_name)

    s3_uri = script_location.split("//")[1]
    bucket = s3_uri.split('/')[0]
    prefix = '/'.join([str(elem) for elem in s3_uri.split('/')[1:]])

    zipped_keys = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    file_list = []
    for key in zipped_keys['Contents']:
        file_list.append(key['Key'])

    # This will give you list of files in the folder you mentioned as prefix
    s3_resource = boto3.resource('s3')

    # Now create zip object one by one, this below is for 1st file in file_list
    zip_obj = s3_resource.Object(bucket_name=bucket, key=file_list[0])
    print(zip_obj)
    buffer = BytesIO(zip_obj.get()["Body"].read())

    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket='cv-marketing-{}'.format(env),
            Key=Key_unzip + f'{filename}')
    # return "s3://"+'cv-marketing-{}'.format(env)+"/"+Key_unzip
    return "s3://" + bucket + "/" + Key_unzip


def start_execution(glue_parameters, glue_opt_params, glue_dpus):
    logger.info("Starting Glue Job Deployemnet")

    logger.info("Check if the script points to a zip file")
    job_name = glue_parameters['glueJobName']
    isScriptZip = glue_parameters["glueScriptLocation"].split('.')[-1] == 'zip'
    if isScriptZip:
        glue_parameters["glueScriptLocation"] = extract_zip(job_name, glue_parameters["glueScriptLocation"],
                                                            glue_parameters['environment']) + job_name + '.txt'
        logger.info("Glue ZIP extracted at {}".format(glue_parameters["glueScriptLocation"]))
    else:
        logger.info("Script location is not zip file ")

    glue_job_deployment(glue_parameters, glue_opt_params, glue_dpus)

    time.sleep(2)

    start_job = glue.start_job_run(
        JobName=job_name,
        Arguments=glue_opt_params)

    run_id = start_job["JobRunId"]

    t = 0
    status = "RUNNING"

    try:
        while status == "RUNNING":
            status = glue_job_status(job_name, run_id)
            print("Time: %i | Status %s" % (t, status))
            time.sleep(5)
            t = t + 5
    except (Exception, KeyboardInterrupt) as e:
        response = glue.batch_stop_job_run(
            JobName=job_name,
            JobRunIds=[run_id]
        )
        logger.info("Recieved Kill Signal as {} . GraceFully Terminated the Job {}".format(e, response))

    if status == "SUCCEEDED":
        logger.info("Job Completed")
        sys.exit("0")
    else:
        logger.error("Job Failed")
        sys.exit("1")


job = dict(config.items('JOB'))

logger.info("Execution Environmnet for spark jobs {}".format(job['Execution_Enviornment']))

# Fetching Job Parameters (Mandatory)
try:
    glue_params = dict(config.items('job-paramters'))
except NoSectionError as e:
    logger.warn('Specify the Mandatory Glue Job paramters')

# Fetching Script Language (Mandatory)
try:
    glue_script_params = dict(config.items('script-language'))
except NoSectionError as e:
    logger.warn('Need to specify --job-language= python or --job-language= scala')

# Fetching DPU/Resource (Mandatory)
try:
    glue_dpus = dict(config.items('job-dpu'))
except NoSectionError as e:
    logger.warn('Need to specify WorkerType & NumberOfWorkers')

# Fetching Job Specific/Optional Paramters
try:
    glue_opt_params = dict(config.items('opt-paramters'))
    glue_script_params.update(glue_opt_params)
except (NoSectionError, NameError) as e:
    logger.info('No Optional Paramter Specified')

logger.info("Starting Execution")
start_execution(glue_params, glue_script_params, glue_dpus)
