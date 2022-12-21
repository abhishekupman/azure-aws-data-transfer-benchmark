package com.adobe.data.tf;

import com.adobe.data.tf.model.ApplicationInput;
import com.adobe.data.tf.model.FinalUploadResult;
import com.newrelic.api.agent.NewRelic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NewRelicUtils {
    private final static Integer BYTES_IN_MB = 1024*1024;
    public static void fillParamsFromApplicationInput(ApplicationInput applicationInput) {
        NewRelic.addCustomParameter("blob_name", applicationInput.getBlobName());
        NewRelic.addCustomParameter("azure_account", applicationInput.getStorageAccount());
        NewRelic.addCustomParameter("chunk_size_in_mb", applicationInput.getChunkSizeInMB());
        NewRelic.addCustomParameter("accelerated_upload", applicationInput.getUseAcceleratedUpload());
    }

    public static void fillParamsFromUploadResult(FinalUploadResult finalUploadResult) {
        NewRelic.addCustomParameter("chunk_upload_count", finalUploadResult.getPartUploadResults().size());
        NewRelic.addCustomParameter("file_size_in_mb", finalUploadResult.getFileSizeInBytes()/BYTES_IN_MB);
        NewRelic.addCustomParameter("total_upload_time_sec", finalUploadResult.getTotalTimeTakenInMillSec()/1000);
        NewRelic.addCustomParameter("all_parts_upload_time_sec", finalUploadResult.getPartsUploadTimeTakenInMillSec()/1000);
        NewRelic.addCustomParameter("init_upload_api_call_time_in_milli_sec", finalUploadResult.getInitUploadApiCallTimeInMillSec());
        NewRelic.addCustomParameter("complete_upload_api_call_time_in_milli_sec", finalUploadResult.getCompleteMultiPartApiCallTimeInMillSec());
        NewRelic.addCustomParameter("upload_bandwidth_achieved", getBandwidthInMBPs(finalUploadResult));
    }

    private static int getBandwidthInMBPs(FinalUploadResult finalUploadResult) {
        long fileSizeInMB = finalUploadResult.getFileSizeInBytes()/BYTES_IN_MB;
        long totalTimeTakenInSec  = finalUploadResult.getTotalTimeTakenInMillSec()/1000;
        int bandwidthInMBps = (int)(fileSizeInMB/totalTimeTakenInSec);
        log.info("Upload bandwidth achieved {} MBps", bandwidthInMBps);
        return bandwidthInMBps;
    }
}
