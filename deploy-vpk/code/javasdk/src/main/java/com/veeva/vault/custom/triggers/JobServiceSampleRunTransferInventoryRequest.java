package com.veeva.vault.custom.triggers;

import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.data.RecordEvent;
import com.veeva.vault.sdk.api.data.RecordTrigger;
import com.veeva.vault.sdk.api.data.RecordTriggerContext;
import com.veeva.vault.sdk.api.data.RecordTriggerInfo;
import com.veeva.vault.sdk.api.job.JobParameters;
import com.veeva.vault.sdk.api.job.JobService;

@RecordTriggerInfo(object = "transfer_inventory_request__c", events = {RecordEvent.AFTER_INSERT})
public class JobServiceSampleRunTransferInventoryRequest implements RecordTrigger {

    public void execute(RecordTriggerContext recordTriggerContext) {

        // Get the record change event that cause the trigger to be executed
        RecordEvent recordEvent = recordTriggerContext.getRecordEvent();

        // Use the logService to log errors, memory usage and debug information
        LogService logService = ServiceLocator.locate(LogService.class);

        //Get an instance of Job for invoking user actions, such as changing state and starting workflow
        JobService jobService = ServiceLocator.locate(JobService.class);
        JobParameters jobParameters = jobService.newJobParameters("transfer_product_to_new_store__c");


        if (recordEvent.toString().equals("AFTER_INSERT")) {

            recordTriggerContext.getRecordChanges().forEach(rc -> {
                // Check newly created transfer inventory request record is in the initial state (pending__c)
                // And add the current and new bicycle store to the job parameters
                logService.info("RC: " + rc.getNew().getValue("transfer_inventory_request_status__c", ValueType.PICKLIST_VALUES));
                if (rc.getNew().getValue("transfer_inventory_request_status__c", ValueType.PICKLIST_VALUES).contains("pending__c")) {
                    jobParameters.setValue("newBicycleStoreId", rc.getNew().getValue("new_bicycle_store__c", ValueType.STRING));
                    jobParameters.setValue("existingBicycleStoreId", rc.getNew().getValue("existing_bicycle_store__c", ValueType.STRING));
                    jobService.run(jobParameters);
                }
            });
        }
    }
}