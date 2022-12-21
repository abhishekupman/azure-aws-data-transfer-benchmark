package com.adobe.data.tf.model;

import com.azure.storage.blob.models.BlobRange;
import lombok.ToString;
import lombok.Value;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@Value
@ToString
public class PartUploadContext {
    int partNumber;
    UploadPartRequest uploadPartRequest;
    BlobRange blobRange;
}
