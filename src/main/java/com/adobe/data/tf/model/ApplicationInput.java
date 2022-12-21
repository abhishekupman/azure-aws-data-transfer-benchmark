package com.adobe.data.tf.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
public class ApplicationInput {
    String storageAccount;
    String containerName;
    String blobName;
    String s3Bucket;
    String s3PrefixKey;
    Integer iterationCount;
    Integer chunkSizeInMB;
    Boolean useAcceleratedUpload;
}
