package com.adobe.data.tf;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class SingleFileUpload {
    private final static String STORAGE_ACCOUNT = "azdatatf";
    private final static String CONTAINER_NAME = "test-data-local";
    private final static String BLOB_NAME = "first_test.txt";

    private final static String S3_BUCKET = "byos-test-bucket";
    private final static String S3_OBJECT_KEY_PREFIX = "azure-data/";

    public static void main(String[] args) {
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder().
                credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

        Mono<PutObjectResponse> uploadResponse = getBytesFromAzure().flatMap(response -> uploadStreamToS3(s3AsyncClient, response));
        uploadResponse.block();
    }

    private static Mono<PutObjectResponse> uploadStreamToS3(S3AsyncClient s3AsyncClient, BlobDownloadAsyncResponse response) {
        Long contentLength = response.getDeserializedHeaders().getContentLength();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(S3_BUCKET)
                .key(S3_OBJECT_KEY_PREFIX + BLOB_NAME)
                .contentLength(contentLength)
                .build();
        AsyncRequestBody requestBody = AsyncRequestBody.fromPublisher(response.getValue());
        return Mono.fromCompletionStage(s3AsyncClient.putObject(request, requestBody));
    }


    private static Mono<BlobDownloadAsyncResponse> getBytesFromAzure() {
        final String accountUrl = "https://"+ STORAGE_ACCOUNT + ".blob.core.windows.net";
        DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder()
                .build();

        BlobServiceAsyncClient client = new BlobServiceClientBuilder()
                .credential(defaultAzureCredential)
                .endpoint(accountUrl)
                .buildAsyncClient();

        BlobContainerAsyncClient blobContainerClient = client.getBlobContainerAsyncClient(CONTAINER_NAME);
        BlobAsyncClient blobAsyncClient = blobContainerClient.getBlobAsyncClient(BLOB_NAME);

        return blobAsyncClient.downloadStreamWithResponse(null, null, null, false);
    }
}
