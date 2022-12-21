package com.adobe.data.tf;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.internal.crt.S3NativeClientConfiguration;

public class S3Utils {
    public static S3AsyncClient getS3AsyncClient(AwsBasicCredentials awsBasicCredentials, boolean acceleratedUpload) {
        //ClientAsyncConfiguration.builder().advancedOption()
        HttpClient build1 = new NettyAsyncHttpClientBuilder().
                build();
        return S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .accelerate(acceleratedUpload)
                .region(Region.US_EAST_1)
                .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(500))
                .build();
    }
}
