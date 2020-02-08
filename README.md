# teslacam-batch-orchestration
This is the Azure Function that takes care of the Azure Batch orchestration.  
Part of my automated TeslaCam video processing solution.

Check the overview blog post here:  
https://www.moderndata.ai/2020/01/automatic-teslacam-and-sentry-mode-video-processing-in-azure-part-1/

This use this Function App in Azure, it requires the following configuration values:
1. _BATCH_ACCOUNT_KEY   
*Put the value of your Batch Account Key here.*
2. _BATCH_ACCOUNT_NAME  
*Put the value of your Batch Account Name here.*
3. _BATCH_ACCOUNT_URL  
*Put the value of your Batch Account URL here.*
4. _STORAGE_ACCOUNT_KEY  
*Put the value of your Storage Account Key here.*
5. _STORAGE_ACCOUNT_NAME  
*Put the value of your  Storage Account Name here.*
6. _JOB_ID  
*Give it the value: DTDJob.*
7. _POOL_ID  
*Give it the value: DTDPool.*
8. _POOL_VM_SIZE  
*Give it the value: STANDARD_A1_v2.*
