package com.adobe.data.tf;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.implementation.util.ChunkedDownloadUtils;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class MultipartFileUploadTest {
    private final static String STORAGE_ACCOUNT = "azdatatf";
    private final static String CONTAINER_NAME = "test-data-local";
    private final static String BLOB_NAME = "23MB.mp4";

    private final static String S3_BUCKET = "byos-test-bucket";
    private final static String S3_OBJECT_KEY_PREFIX = "azure-data/";

    private final static long CHUNK_SIZE = 10*1024*1024;

    public static void main(String[] args) {
        S3AsyncClient s3AsyncClient = getS3AsyncClient();
        BlobAsyncClient azureBlobClient = getAzureBlobClient();
        BlobProperties block = azureBlobClient.getProperties().block();
        log.info("blob size {} md5 {}", block.getBlobSize(), new String(block.getContentMd5()));
        int chunkCount = ChunkedDownloadUtils.calculateNumBlocks(block.getBlobSize(), CHUNK_SIZE);
        log.info("chunk count {}", chunkCount);

        List<BlobRange> rangeList = IntStream.range(0, chunkCount).mapToObj(val -> {
            Long offset = val * CHUNK_SIZE;
            Long byteCount = Math.min(CHUNK_SIZE, block.getBlobSize() - offset);
            log.info("offset {} and Bytecount is {}", offset, byteCount);
            return new BlobRange(offset, byteCount);
        }).collect(Collectors.toList());
        System.out.println(rangeList);

        CreateMultipartUploadResponse initUploadResponse = Mono.fromCompletionStage(s3AsyncClient.createMultipartUpload(CreateMultipartUploadRequest.builder().
                bucket(S3_BUCKET).
                key(S3_OBJECT_KEY_PREFIX + BLOB_NAME).
                build())).block();

        List<CompletedPart> completedPart = new ArrayList<>();

        for (int i = 0; i < chunkCount; i++) {
            Long offset = i * CHUNK_SIZE;
            Long byteCount = Math.min(CHUNK_SIZE, block.getBlobSize() - offset);

            BlobRange blobRange = new BlobRange(offset, byteCount);

            UploadPartRequest s3PartRequest = UploadPartRequest.builder().
                    bucket(initUploadResponse.bucket()).
                    key(initUploadResponse.key()).
                    uploadId(initUploadResponse.uploadId()).
                    partNumber(i+1).
                    contentLength(byteCount).
                    build();
            log.info("byte range {} Starting part upload part {} ", blobRange, i+1);

            BlobDownloadAsyncResponse block1 = azureBlobClient.downloadStreamWithResponse(blobRange, null, null, false).block();
            UploadPartResponse partResponse = Mono.fromCompletionStage(s3AsyncClient.uploadPart(s3PartRequest, AsyncRequestBody.fromPublisher(block1.getValue()))).block();
            CompletedPart part = CompletedPart.builder().eTag(partResponse.eTag()).
                    partNumber(i + 1).
                    checksumCRC32(partResponse.checksumCRC32()).
                    checksumSHA1(partResponse.checksumSHA1()).
                    checksumSHA256(partResponse.checksumSHA256()).
                    checksumCRC32C(partResponse.checksumCRC32C()).
                    build();
            completedPart.add(part);
            log.info("part {}", part);
            log.info("completed byte range {} Starting part upload part {} with etag {} ", blobRange, i+1, partResponse.eTag());
        }

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder().
                bucket(initUploadResponse.bucket()).
                key(initUploadResponse.key()).
                uploadId(initUploadResponse.uploadId()).
                multipartUpload(CompletedMultipartUpload.builder().
                        parts(completedPart).
                        build()).
                build();
        CompleteMultipartUploadResponse completeResponse = Mono.fromCompletionStage(s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest)).block();
        log.info("response etag {}", completeResponse.eTag());
    }

    private static S3AsyncClient getS3AsyncClient() {
        return S3AsyncClient.builder().
                credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    private static BlobAsyncClient getAzureBlobClient() {
        final String accountUrl = "https://"+ STORAGE_ACCOUNT + ".blob.core.windows.net";
        DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder()
                .build();

        BlobServiceAsyncClient client = new BlobServiceClientBuilder()
                .credential(defaultAzureCredential)
                .endpoint(accountUrl)
                .buildAsyncClient();

        BlobContainerAsyncClient blobContainerClient = client.getBlobContainerAsyncClient(CONTAINER_NAME);
        return blobContainerClient.getBlobAsyncClient(BLOB_NAME);
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
