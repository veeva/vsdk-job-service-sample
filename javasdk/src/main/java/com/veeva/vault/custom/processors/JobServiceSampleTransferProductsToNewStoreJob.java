package com.veeva.vault.custom.processors;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.PositionalRecordId;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.job.*;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;

import java.math.BigDecimal;
import java.util.List;

@JobInfo(adminConfigurable = true)
public class JobServiceSampleTransferProductsToNewStoreJob implements Job {

    // Initialize custom job and set job input values
    public JobInputSupplier init(JobInitContext jobInitContext) {

        // Locate the Query Service used to find bicycle accessories and store data
        QueryService queryService = ServiceLocator.locate(QueryService.class);

        // Get the jobLogger used to log debug information
        JobLogger logger = jobInitContext.getJobLogger();

        // Get the job parameters
        String existingBicycleStoreId = jobInitContext.getJobParameter("existingBicycleStoreId", JobParamValueType.STRING);
        String newBicycleStoreId = jobInitContext.getJobParameter("newBicycleStoreId", JobParamValueType.STRING);

        // Create a new list of JobItem types to hold all the job items
        // The Suppress Warnings annotation is used to suppress compiler warnings
        // In this case, the unchecked warning is suppressed. This suppress type safety warnings.
        // This needs to be done as List is a generic type.
        @SuppressWarnings("unchecked")
        List<JobItem> jobItems = VaultCollections.newList();

        // Get ID's of all Bicycle accessories sold by the old store
        StringBuilder bicycleAccessoryQuerySB = new StringBuilder()
                .append("SELECT id, name__v, price__c, quantity__c FROM bicycle_accessory__c WHERE bicycle_store__cr.id='")
                .append(existingBicycleStoreId)
                .append("' LIMIT 1");

        logger.log("Running Bicycle Accessory VQL query: " + bicycleAccessoryQuerySB.toString());
        QueryResponse accessoryQueryResponse = queryService.query(bicycleAccessoryQuerySB.toString());
        logger.log("Bicycle Accessory VQL query returned " + accessoryQueryResponse.getResultCount() + " results");

        // Iterate over Bicycle Accessory Query Results
        if(accessoryQueryResponse.getResultCount() > 0 && newBicycleStoreId != null) {
            accessoryQueryResponse.streamResults().forEach(queryResult -> {
                JobItem bicycleAccessoryItem = jobInitContext.newJobItem();

                // Get id, name, price and quantity of the bicycle accessory
                String bicycleAccessoryId = queryResult.getValue("id", ValueType.STRING);
                String bicycleAccessoryName = queryResult.getValue("name__v", ValueType.STRING);
                BigDecimal bicycleAccessoryPrice = queryResult.getValue("price__c", ValueType.NUMBER);
                BigDecimal bicycleAccessoryQuantity = queryResult.getValue("quantity__c", ValueType.NUMBER);

                // Set JobItem values
                bicycleAccessoryItem.setValue("bicycleAccessoryId", bicycleAccessoryId);
                bicycleAccessoryItem.setValue("bicycleAccessoryName", bicycleAccessoryName);
                bicycleAccessoryItem.setValue("bicycleAccessoryPrice", bicycleAccessoryPrice);
                bicycleAccessoryItem.setValue("bicycleAccessoryQuantity", bicycleAccessoryQuantity);
                bicycleAccessoryItem.setValue("newBicycleStoreId", newBicycleStoreId);

                logger.log("Added new job item,  bicycleAccessoryId: " + ",  newBicycleStoreId: " + newBicycleStoreId);
                jobItems.add(bicycleAccessoryItem);
            });
            logger.log("Job Input Items: " + jobItems.toString());
            return jobInitContext.newJobInput(jobItems);
        }
        logger.log("Job Input is empty");
        return jobInitContext.newJobInput(jobItems);
    }

    // Process Job Items and set task output status
    public void process(JobProcessContext jobProcessContext) {

        // Locate the RecordService used to create the clone records and delete the old ones
        RecordService recordService = ServiceLocator.locate(RecordService.class);

        // Get the JobLogger used to log debug information
        JobLogger logger = jobProcessContext.getJobLogger();

        // Fetch all the JobItems for the current job task
        List<JobItem> bicycleAccessoryItems = jobProcessContext.getCurrentTask().getItems();

        // Create a list for the records that are to be deleted
        @SuppressWarnings("unchecked")
        List<Record> deletedBicycleAccessoryRecords = VaultCollections.newList();

        // Create a list for the clone records
        @SuppressWarnings("unchecked")
        List<Record> clonedBicycleAccessoryRecords = VaultCollections.newList();

        // Iterate over the job items
        for (JobItem bicycleAccessoryItem : bicycleAccessoryItems) {

            // Get the existing bicycle accessory object record id
            String bicycleAccessoryId = bicycleAccessoryItem.getValue("bicycleAccessoryId", JobValueType.STRING);

            // Get the new store record id
            String newBicycleStoreId = bicycleAccessoryItem.getValue("newBicycleStoreId", JobValueType.STRING);

            // If both the current accessory and the new store record's exist
            if(bicycleAccessoryId != null && newBicycleStoreId != null) {
                // Initialize Record with existing Id
                Record oldBicycleAccessoryRecord = recordService.newRecordWithId("bicycle_accessory__c", bicycleAccessoryId);

                logger.log("Creating clone record");
                Record cloneBicycleAccessoryRecord = recordService.newRecord("bicycle_accessory__c");

                logger.log("Setting bicycle_store__c on clone record");
                cloneBicycleAccessoryRecord.setValue("bicycle_store__c", newBicycleStoreId);

                // Get JobItems set in Init method
                String bicycleAccessoryName = bicycleAccessoryItem.getValue("bicycleAccessoryName", JobValueType.STRING);
                BigDecimal bicycleAccessoryPrice = bicycleAccessoryItem.getValue("bicycleAccessoryPrice", JobValueType.NUMBER);
                BigDecimal bicycleAccessoryQuantity = bicycleAccessoryItem.getValue("bicycleAccessoryQuantity", JobValueType.NUMBER);

                // Set values on new record
                logger.log("Setting name__v on clone record");
                cloneBicycleAccessoryRecord.setValue("name__v", bicycleAccessoryName);

                logger.log("Setting price__v on clone record");
                cloneBicycleAccessoryRecord.setValue("price__c", bicycleAccessoryPrice);

                logger.log("Setting quantity__v on clone record");
                cloneBicycleAccessoryRecord.setValue("quantity__c", bicycleAccessoryQuantity);

                // Add new record to list to be saved
                logger.log("Record clone has been created");
                clonedBicycleAccessoryRecords.add(cloneBicycleAccessoryRecord);

                // Add old record to list to be deleted
                logger.log("Deleting bicycle record with id: " + bicycleAccessoryId);
                deletedBicycleAccessoryRecords.add(oldBicycleAccessoryRecord);
            }
        }

        JobTask task = jobProcessContext.getCurrentTask();
        TaskOutput taskOutput = task.getTaskOutput();

        // Batch save new records
        BatchOperation<PositionalRecordId, BatchOperationError> batchSaveResult = recordService.batchSaveRecords(clonedBicycleAccessoryRecords);

        // Batch delete old records
        BatchOperation<PositionalRecordId, BatchOperationError> batchDeleteResult = recordService.batchDeleteRecords(deletedBicycleAccessoryRecords);

        batchSaveResult.onSuccesses(positionalRecordIds -> {
            taskOutput.setState(TaskState.SUCCESS);
            logger.log("Clone Record Save successful");
        });
        batchDeleteResult.onSuccesses(positionalRecordIds -> {
            taskOutput.setState(TaskState.SUCCESS);
            logger.log("Old Record Delete successful");
        });

        batchSaveResult.onErrors( batchOperationErrors -> {
            taskOutput.setState(TaskState.ERRORS_ENCOUNTERED);
            taskOutput.setValue("firstError", batchOperationErrors.get(0).getError().getMessage());
            logger.log("Clone Record Save unsuccessful due to: " + batchOperationErrors.get(0).getError().getMessage());
        });
        batchDeleteResult.onErrors( batchOperationErrors -> {
            taskOutput.setState(TaskState.ERRORS_ENCOUNTERED);
            taskOutput.setValue("firstError", batchOperationErrors.get(0).getError().getMessage());
            logger.log("Old Record Delete unsuccessful due to: " + batchOperationErrors.get(0).getError().getMessage());
        });

        batchSaveResult.execute();
        batchDeleteResult.execute();
    }

    public void completeWithSuccess(JobCompletionContext jobCompletionContext) {
        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("All tasks completed successfully");
    }

    public void completeWithError(JobCompletionContext jobCompletionContext) {
        JobResult result = jobCompletionContext.getJobResult();

        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("completeWithError: " + result.getNumberFailedTasks() + "tasks failed out of " + result.getNumberTasks());

        List<JobTask> tasks = jobCompletionContext.getTasks();
        for (JobTask task : tasks) {
            TaskOutput taskOutput = task.getTaskOutput();
            if (TaskState.ERRORS_ENCOUNTERED.equals(taskOutput.getState())) {
                logger.log(task.getTaskId() + " failed with error message " + taskOutput.getValue("firstError", JobValueType.STRING));
            }
        }
    }
}