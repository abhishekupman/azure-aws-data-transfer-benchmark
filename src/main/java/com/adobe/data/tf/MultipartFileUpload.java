package com.adobe.data.tf;

import com.adobe.data.tf.model.*;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.implementation.util.ChunkedDownloadUtils;
import com.azure.storage.blob.models.BlobDownloadAsyncResponse;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class MultipartFileUpload {

    public static List<FinalUploadResult> testMultiPartUploadToS3(ApplicationInput appInput) {
        BlobAsyncClient azureBlobClient = AzureUtils.getAzureBlobClient(appInput);
        AwsBasicCredentials awsBasicCredentials = AzureUtils.fetchAWSCredentialsFromKeyVault();
        S3AsyncClient s3AsyncClient = S3Utils.getS3AsyncClient(awsBasicCredentials, appInput.getUseAcceleratedUpload());

        List<FinalUploadResult> finalUploadResults = new ArrayList<>();
        for (int i = 0; i < appInput.getIterationCount(); i++) {
            finalUploadResults.add(doMultiPartUpload(azureBlobClient, s3AsyncClient, appInput));
        }
        return finalUploadResults;
    }


    @Trace(dispatcher = true)
    private static FinalUploadResult doMultiPartUpload(BlobAsyncClient azureBlobClient, S3AsyncClient s3AsyncClient, ApplicationInput appInput) {
        NewRelic.setTransactionName("App", "NewUploadV1");
        NewRelicUtils.fillParamsFromApplicationInput(appInput);

        String s3Bucket = appInput.getS3Bucket();
        String formattedPath = generatePrefixForS3Key();
        String s3PrefixKey = appInput.getS3PrefixKey() +"/"+ formattedPath +"/"+appInput.getBlobName();

        FinalUploadResult finalUploadResult = new FinalUploadResult();
        long start = System.currentTimeMillis();
        getBlobProperties(finalUploadResult, azureBlobClient).zipWith(initUploadRequestToS3(finalUploadResult, s3AsyncClient, s3Bucket, s3PrefixKey))
                .flatMap(tuple -> doMultiPartUpload(finalUploadResult, s3AsyncClient, azureBlobClient, tuple.getT1(), tuple.getT2(), appInput.getChunkSizeInMB() * 1024 * 1024))
                .doOnNext(res -> finalUploadResult.setTotalTimeTakenInMillSec(getElapsedTimeInMill(start)))
                .doOnNext(res -> log.info("Completed multipart stream from azure to s3"))
                .log()
                .block();
        log.info("done uploading on s3 {}", s3PrefixKey);

        NewRelic.addCustomParameter("s3_upload_path", s3PrefixKey);
        NewRelicUtils.fillParamsFromUploadResult(finalUploadResult);
        return finalUploadResult;
    }

    private static Mono<CreateMultipartUploadResponse> initUploadRequestToS3(FinalUploadResult finalUploadResult, S3AsyncClient s3AsyncClient, String s3Bucket, String s3Key) {
        long start = System.currentTimeMillis();
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder().
                bucket(s3Bucket).
                key(s3Key).
                build();

        return Mono.fromCompletionStage(s3AsyncClient.createMultipartUpload(createRequest)).
                doOnSubscribe(req -> log.info("S3 Init Upload Start")).
                doOnNext(req -> log.info("S3 Init Upload Called on s3 prefix key {}", req.key())).
                // hack to capture initUploadTime
                doOnNext(req -> finalUploadResult.setInitUploadApiCallTimeInMillSec(getElapsedTimeInMill(start))).
                subscribeOn(Schedulers.boundedElastic());
    }

    private static Mono<BlobProperties> getBlobProperties(FinalUploadResult finalUploadResult, BlobAsyncClient azureBlobClient) {
        return azureBlobClient.getProperties().
                doOnSubscribe(req -> log.info("Fetching Blob MetaData")).
                doOnNext(req -> log.info("Fetch Blob MetaData Done")).
                doOnNext(req -> finalUploadResult.setFileSizeInBytes(req.getBlobSize())).
                subscribeOn(Schedulers.boundedElastic());
    }

    private static Mono<CompleteMultipartUploadResponse> doMultiPartUpload(FinalUploadResult finalUploadResult, S3AsyncClient s3AsyncClient,
                                                                           BlobAsyncClient azureBlobClient, BlobProperties blobProperties,
                                                                           CreateMultipartUploadResponse initUploadResponse, long chunkSize) {
        long start = System.currentTimeMillis();
        long blobSize = blobProperties.getBlobSize();
        int chunkCount = ChunkedDownloadUtils.calculateNumBlocks(blobSize, chunkSize);
        log.info("chunk count {}", chunkCount);
        return Flux.range(0, chunkCount).
                map(num -> {
                    long offset = num * chunkSize;
                    Long byteCount = Math.min(chunkSize, blobSize - offset);
                    BlobRange blobRange = new BlobRange(offset, byteCount);
                    UploadPartRequest s3PartRequest = UploadPartRequest.builder().
                            bucket(initUploadResponse.bucket()).
                            key(initUploadResponse.key()).
                            uploadId(initUploadResponse.uploadId()).
                            partNumber(num+1).
                            contentLength(byteCount).
                            build();
                    return new PartUploadContext(num+1, s3PartRequest, blobRange);
                }).
                flatMap(partUploadContext -> uploadPart(s3AsyncClient, azureBlobClient, partUploadContext)).
                // wait for all part to complete
                collectList().
                doOnNext(list -> {
                    List<PartUploadResult> partUploadResults = list.stream().
                            map(PartUploadSummary::getPartUploadResult).
                            collect(Collectors.toList());
                    finalUploadResult.setPartUploadResults(partUploadResults);
                    finalUploadResult.setPartsUploadTimeTakenInMillSec(getElapsedTimeInMill(start));
                }).
                map(partUploadSummaries -> partUploadSummaries.stream().
                        map(PartUploadSummary::getCompletedPart).
                        collect(Collectors.toList())
                ).flatMap(completedParts -> completeMultiPartUpload(finalUploadResult, s3AsyncClient, completedParts, initUploadResponse));
    }


    private static Mono<CompleteMultipartUploadResponse> completeMultiPartUpload(FinalUploadResult finalUploadResult, S3AsyncClient s3AsyncClient,
                                                                                 List<CompletedPart> completedParts, CreateMultipartUploadResponse initUploadResponse) {
        // sort the parts in order
        long start = System.currentTimeMillis();
        completedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder().
                bucket(initUploadResponse.bucket()).
                key(initUploadResponse.key()).
                uploadId(initUploadResponse.uploadId()).
                multipartUpload(CompletedMultipartUpload.builder().
                        parts(completedParts).
                        build()).
                build();
        log.info("Calling Finalize API call to complete Multipart Upload");
        val response = s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest);
        return Mono.fromCompletionStage(response).
                doOnNext(res -> log.info("Completed the S3 Multipart Upload")).
                doOnNext(res -> finalUploadResult.setCompleteMultiPartApiCallTimeInMillSec(getElapsedTimeInMill(start)));
    }

    private static Mono<PartUploadSummary> uploadPart(S3AsyncClient s3AsyncClient, BlobAsyncClient azureBlobClient, PartUploadContext partUploadContext) {
        long startTime = System.currentTimeMillis();
        return azureBlobClient.downloadStreamWithResponse(partUploadContext.getBlobRange(), null, null, false).
                flatMap(azureBlobResponse -> UploadAzureStreamToS3UploadPart(s3AsyncClient, partUploadContext, azureBlobResponse)).
                map(uploadPartResponse -> CompletedPart.builder().eTag(uploadPartResponse.eTag()).
                            partNumber(partUploadContext.getPartNumber()).
                            checksumCRC32(uploadPartResponse.checksumCRC32()).
                            checksumSHA1(uploadPartResponse.checksumSHA1()).
                            checksumSHA256(uploadPartResponse.checksumSHA256()).
                            checksumCRC32C(uploadPartResponse.checksumCRC32C()).
                            build()
                ).map(completedPart -> {
                    // hack to capture part upload time
                    PartUploadResult partUploadResult = new PartUploadResult(partUploadContext.getUploadPartRequest().contentLength(), getElapsedTimeInMill(startTime));
                    return new PartUploadSummary(partUploadResult, completedPart);
                });
    }

    private static Mono<UploadPartResponse> UploadAzureStreamToS3UploadPart(S3AsyncClient s3AsyncClient, PartUploadContext partUploadContext, BlobDownloadAsyncResponse azureBlobResponse) {
        log.info("Starting upload for part number {}", partUploadContext.getPartNumber());
        val uploadPartResponse = s3AsyncClient.uploadPart(partUploadContext.getUploadPartRequest(),
                AsyncRequestBody.fromPublisher(azureBlobResponse.getValue()));
        return Mono.fromCompletionStage(uploadPartResponse)
                .doOnNext(response -> log.info("completed upload for part {} with eTag {}", partUploadContext.getPartNumber(), response.eTag()));
    }

    private static String generatePrefixForS3Key() {
        Format formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        return formatter.format(new Date());
    }

    private static long getElapsedTimeInMill(long startTime) {
        return System.currentTimeMillis() - startTime;
    }

}
