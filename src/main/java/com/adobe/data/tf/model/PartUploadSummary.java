package com.adobe.data.tf.model;

import lombok.Value;
import software.amazon.awssdk.services.s3.model.CompletedPart;

@Value
public class PartUploadSummary {
    PartUploadResult partUploadResult;
    CompletedPart completedPart;
}
