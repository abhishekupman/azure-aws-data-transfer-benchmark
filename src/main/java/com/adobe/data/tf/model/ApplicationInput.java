package com.adobe.data.tf.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
public class ApplicationInput {
    private final String storageAccount;
    private final String containerName;
    private final String blobName;
    private final String s3Bucket;
    private final String s3PrefixKey;
    private final Integer iterationCount;
    private final Integer chunkSizeInMB;
}
