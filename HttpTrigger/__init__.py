import azure.functions as func
import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

import datetime
import io
import os
import sys
import time
import logging
from datetime import timedelta

try:
    input = raw_input
except NameError:
    pass

def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    #print('-------------------------------------------')
    logging.error('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        logging.error(batch_exception.error.message.value)
        if batch_exception.error.values:
            #print()
            for mesg in batch_exception.error.values:
                logging.error('{}:\t{}'.format(mesg.key, mesg.value))
    #print('-------------------------------------------')

def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately. Expiration is in 2 hours.
    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    return container_sas_token


def get_container_sas_url(block_blob_client,
                          container_name, blob_permissions):
    """
    Obtains a shared access signature URL that provides write access to the 
    ouput container to which the tasks will upload their output.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS URL granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container.
    sas_token = get_container_sas_token(block_blob_client,
                                        container_name, azureblob.BlobPermissions.WRITE)

    # Construct SAS URL for the container
    container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(
        os.environ["_STORAGE_ACCOUNT_NAME"], container_name, sas_token)

    return container_sas_url


def create_or_update_pool(batch_service_client, pool_id, num_tasks):
    """
    Creates or updates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param num_tasks: the number of tasks that will be added to the pool later (to compute amount of nodes).
    """
    logging.info('Creating pool [{}]...'.format(pool_id))
 
    maxNumberofVMs = 25
    pool_size = min(maxNumberofVMs, num_tasks)

    if batch_service_client.pool.exists(pool_id) == False:
        new_pool = batch.models.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="Canonical",
                    offer="UbuntuServer",
                    sku="18.04-LTS",
                    version="latest"
                ),
                node_agent_sku_id="batch.node.ubuntu 18.04"),
            vm_size=os.environ["_POOL_VM_SIZE"],
            enable_auto_scale=True,
            auto_scale_evaluation_interval=timedelta(days=0,hours=0,minutes=5),
            auto_scale_formula='''// Sets the initial pool size:
initialPoolSize = {poolsize};
$TargetLowPriorityNodes = initialPoolSize;
// "Mon, 06 Oct 2014 10:20:00 GMT" represents the datetime that this autoscale formula starts to evaluate. This is an arbitrary value here.
lifespan = time() - time("Mon, 06 Oct 2014 10:20:00 GMT");
// Representing 15 minutes
span = TimeInterval_Minute * 15;
// Representing 10 minutes
startup = TimeInterval_Minute * 10;
ratio = 50;

// After 10 minutes, obtains the max value of the number of running and active tasks within the past 15 minutes.
// If both values are 0 (indicating that no tasks were running or active in the last 15 minutes), the pool size is set to 0.
// If either value is greater than zero, no change is made.
$TargetLowPriorityNodes = (lifespan > startup ? (max($RunningTasks.GetSample(span, ratio), 
$ActiveTasks.GetSample(span, ratio)) == 0 ? 0 : $TargetLowPriorityNodes) : initialPoolSize );
$NodeDeallocationOption = taskcompletion;'''.format(poolsize=pool_size, now=datetime.datetime.now()),
            start_task=batchmodels.StartTask(
                command_line="/bin/bash -c \"add-apt-repository ppa:deadsnakes/ppa && apt-get update && apt-get install -y ffmpeg python3.7 python3.7-venv python3.7-dev && apt-get install -y python3-pip && apt-get install -f && python3.7 -m pip install --upgrade pip setuptools wheel && pip3 install python-dateutil && pip3 install psutil && pip3 install requests && pip3 install tzlocal && pip3 install tesla-dashcam\"",
                wait_for_success=True,
                user_identity=batchmodels.UserIdentity(
                    auto_user=batchmodels.AutoUserSpecification(
                        scope=batchmodels.AutoUserScope.pool,
                        elevation_level=batchmodels.ElevationLevel.admin)),
            )
        )
        batch_service_client.pool.add(new_pool)
        logging.info('Pool created.')
    else:
        logging.info('Pool already exists! Resizing..')
        pool_resize_parameter = batch.models.PoolResizeParameter(
            target_dedicated_nodes=0,
            target_low_priority_nodes=pool_size
        )
        batch_service_client.pool.resize(pool_id, pool_resize_parameter)

def downsize_pool(batch_service_client, pool_id):
    """
    Downsizes a pool of compute nodes.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    """
    logging.info('Downsizing pool [{}]...'.format(pool_id))

    if batch_service_client.pool.exists(pool_id) == True:
        logging.info('Pool exists! Resizing..')
        pool_resize_parameter = batch.models.PoolResizeParameter(
            target_dedicated_nodes=0,
            target_low_priority_nodes=0
        )
        batch_service_client.pool.resize(pool_id, pool_resize_parameter)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    logging.info('Creating job [{}]...'.format(job_id))
    try:
        job = batch.models.JobAddParameter(
            id=job_id,
            constraints=batchmodels.JobConstraints(max_wall_clock_time=datetime.timedelta(minutes=60),max_task_retry_count=1),
            on_all_tasks_complete=batchmodels.OnAllTasksComplete.terminate_job,
            pool_info=batch.models.PoolInformation(pool_id=pool_id)
        )
        batch_service_client.job.add(job)
    except:
        logging.info('Job already exists (or error occurred during creating, todo: handle exception apart from exists check).')
    finally:
        logging.info('Job creating done.')
    

def add_task(batch_service_client, job_id, input_files, output_container_sas_url, task_name, input_file_dir):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    tasks = list()
    
    # my own tasks section
    logging.info('Adding dashcam task [{task}] to job [{job}]...'.format(task=task_name,job=job_id))
    output_file_path = "/mnt/batch/tasks/workitems/{jobname}/job-1/{taskname}/wd".format(jobname=job_id,taskname=task_name)
    logging.info('input_file_dir: {}'.format(input_file_dir))
    logging.info('output_file_path: {}'.format(output_file_path))

    # Command uses the script from https://github.com/ehendrix23/tesla_dashcam
    command = '''tesla_dashcam --no-timestamp --monitor_trigger {input} --output {output} --motion_only --mirror --layout DIAMOND --fps 33 --quality HIGH --slowdown 5'''.format(input=input_file_dir, output=output_file_path)
    logging.info('command: {}'.format(command))

    tasks.append(
        batch.models.TaskAddParameter(
            id=task_name,
            command_line=command,
            constraints=batchmodels.TaskConstraints(max_wall_clock_time=datetime.timedelta(minutes=60), max_task_retry_count=1),
            resource_files=input_files,
            output_files=[
                batchmodels.OutputFile(
                    file_pattern='*.mp4',
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            path='',
                            container_url=output_container_sas_url #os.environ["_SAS"]
                        )
                    ),
                    upload_options=batchmodels.OutputFileUploadOptions(
                        upload_condition=batchmodels.OutputFileUploadCondition.task_success
                    )
                ),
                batchmodels.OutputFile(
                    file_pattern='../std*.txt',
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            path=task_name,
                            container_url=output_container_sas_url #os.environ["_SAS"]
                        )
                    ),
                    upload_options=batchmodels.OutputFileUploadOptions(
                        upload_condition=batchmodels.OutputFileUploadCondition.task_success
                    )
                ),
                batchmodels.OutputFile(
                    file_pattern='../fileupload*.txt',
                    destination=batchmodels.OutputFileDestination(
                        container=batchmodels.OutputFileBlobContainerDestination(
                            path=task_name,
                            container_url=output_container_sas_url #os.environ["_SAS"]
                        )
                    ),
                    upload_options=batchmodels.OutputFileUploadOptions(
                        upload_condition=batchmodels.OutputFileUploadCondition.task_success
                    )
                )
            ]
        )
    )
    
    batch_service_client.task.add_collection(job_id, tasks)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    start_time = datetime.datetime.now().replace(microsecond=0)
    logging.info('Script start: {}'.format(start_time))
    
    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    blob_client = azureblob.BlockBlobService(
        account_name=os.environ["_STORAGE_ACCOUNT_NAME"],
        account_key=os.environ["_STORAGE_ACCOUNT_KEY"]
    )

    #pool_id = os.environ["_POOL_ID"] + '_{date:%Y-%m-%d_%H-%M-%S}'.format(date=datetime.datetime.now())
    # ^ I used to use a timestamp in the pool_id, but my setup is simple enough to just use a single pool and reuse it.

    pool_id = os.environ["_POOL_ID"]
    job_id = os.environ["_JOB_ID"] + '_{date:%Y-%m-%d_%H-%M-%S}'.format(date=datetime.datetime.now())

    output_container_name = 'output'
    toprocess_container_name = 'toprocess'
    
    #clean up the 'toprocess' container
    blob_client.create_container(toprocess_container_name, fail_on_exist=False)
    logging.info('Container [{}] created if not exists.'.format(toprocess_container_name))
    for blob in blob_client.list_blobs(toprocess_container_name).items:
            blob_client.delete_blob(toprocess_container_name, blob.name)
    logging.info('Clean up of container finished.')

    output_container_sas_url = get_container_sas_url(
        blob_client,
        output_container_name,
        azureblob.BlobPermissions.WRITE)

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batchauth.SharedKeyCredentials(os.environ["_BATCH_ACCOUNT_NAME"],
                                                 os.environ["_BATCH_ACCOUNT_KEY"])

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=os.environ["_BATCH_ACCOUNT_URL"])

    try:
        source_container_name = 'teslacam'
        dest_container_name = toprocess_container_name

        # Create the pool that will contain the compute nodes that will execute the tasks.
        # Check how many tasks will be created:
        sentry_clips = blob_client.list_blobs(source_container_name, prefix='SentryClips/', delimiter='/')
        saved_clips = blob_client.list_blobs(source_container_name, prefix='SavedClips/', delimiter='/')
        num_sentry_clips = len(sentry_clips.items)
        num_saved_clips = len(saved_clips.items)
        num_tasks = num_saved_clips+num_sentry_clips
        logging.info("Number of counted folders (thus tasks): {tasks}.".format(tasks=num_tasks))

        create_or_update_pool(batch_client, pool_id, num_tasks)

        # Create the job that will run the tasks.
        create_job(batch_client, job_id, pool_id)        

        # get list of Sentry Events and create tasks for those:
        for folder in sentry_clips.items:
            # for each event folder, upload the .mp4 files, create SAS urls and create task
            resourcefiles = []
            taskname=folder.name[0:-1].replace("/","_")

            destfolderprefix = folder.name + 'TeslaCam/'
            for file in blob_client.list_blobs(source_container_name, prefix=folder.name, delimiter='/').items:
                # for each file, upload it, create SAS url
                dest_blob_name = destfolderprefix + file.name
                source_sas_token = get_container_sas_token(blob_client, source_container_name, azureblob.BlobPermissions.READ)
                source_sas_url = blob_client.make_blob_url(source_container_name, file.name, sas_token=source_sas_token)
                blob_client.copy_blob(dest_container_name, dest_blob_name, source_sas_url)          
                dest_sas_token = get_container_sas_token(blob_client, dest_container_name, azureblob.BlobPermissions.READ)
                dest_sas_url = blob_client.make_blob_url(dest_container_name, dest_blob_name, sas_token=dest_sas_token)
                resourcefile = batchmodels.ResourceFile(file_path=dest_blob_name, http_url=dest_sas_url)
                resourcefiles.append(resourcefile)
            # create task
            input_file_dir = folder.name[0:-1] + '/TeslaCam'
            add_task(batch_client, job_id, resourcefiles, output_container_sas_url, taskname, input_file_dir)

        logging.info("All Sentry Clips tasks created in Azure Batch.")
        
        # get list of Sentry Events and create tasks for those:
        for folder in saved_clips.items:
            # for each event folder, upload the .mp4 files, create SAS urls and create task
            resourcefiles = []
            taskname=folder.name[0:-1].replace("/","_")

            destfolderprefix = folder.name + 'TeslaCam/'
            for file in blob_client.list_blobs(source_container_name, prefix=folder.name, delimiter='/').items:
                # for each file, upload it, create SAS url
                dest_blob_name = destfolderprefix + file.name
                source_sas_token = get_container_sas_token(blob_client, source_container_name, azureblob.BlobPermissions.READ)
                source_sas_url = blob_client.make_blob_url(source_container_name, file.name, sas_token=source_sas_token)
                blob_client.copy_blob(dest_container_name, dest_blob_name, source_sas_url)          
                dest_sas_token = get_container_sas_token(blob_client, dest_container_name, azureblob.BlobPermissions.READ)
                dest_sas_url = blob_client.make_blob_url(dest_container_name, dest_blob_name, sas_token=dest_sas_token)
                resourcefile = batchmodels.ResourceFile(file_path=dest_blob_name, http_url=dest_sas_url)
                resourcefiles.append(resourcefile)
            # create task
            input_file_dir = folder.name[0:-1] + '/TeslaCam'
            add_task(batch_client, job_id, resourcefiles, output_container_sas_url, taskname, input_file_dir)
        
        logging.info("All Saved Clips tasks created in Azure Batch.")

        # archive videos from teslacam container to archive folder
        logging.info('Started archiving...')
        archive_container_name = 'archive'
        archive_prefix = 'backup_{date:%Y-%m-%d_%H-%M-%S}/'.format(date=datetime.datetime.now())
        for blob in blob_client.list_blobs(source_container_name).items:
            arch_blob_name = archive_prefix + blob.name
            source_sas_token = get_container_sas_token(blob_client, source_container_name, azureblob.BlobPermissions.READ)
            source_sas_url = blob_client.make_blob_url(source_container_name, blob.name, sas_token=source_sas_token)
            blob_client.copy_blob(archive_container_name, arch_blob_name, source_sas_url)
        logging.info('Finished archiving.')

        # Clean 'teslacam' container
        logging.info('Cleaning [{}] container...'.format(source_container_name))
        for blob in blob_client.list_blobs(source_container_name).items:
            blob_client.delete_blob(source_container_name, blob.name)
        logging.info('Clean up of [{}] container finished.'.format(source_container_name))

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    except Exception as err:
        logging.error(err)
        raise

    else:
        # Clean up Batch resources (if the user so chooses).
        logging.info('Script ends here.')
        return func.HttpResponse(f"Script done", status_code=200)

    finally:
        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        logging.info('Job end: {}'.format(end_time))
        logging.info('Elapsed time: {}'.format(end_time - start_time))      
