package com.adobe.data.tf;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestMain {

    private final static String STORAGE_ACCOUNT = "azdatatf";
    private final static String CONTAINER_NAME = "test-data-local";
    private final static String BLOB_NAME = "first_test.txt";

    private final static String KEY_VAULT_NAME = "az-aws-cred";
    private final static String SECRET_NAME = "TestKey";

    public static void main(String[] args) {
        getSecret();
    }

    private static String getSecret() {
        String keyVaultUri = "https://" + KEY_VAULT_NAME + ".vault.azure.net";
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUri)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        KeyVaultSecret secret = secretClient.getSecret(SECRET_NAME);
        String secretVal = secret.getValue();
        log.info("Secret Val "+ secretVal);
        return secretVal;
    }

    public static void main1(String[] args) throws InterruptedException {
        final String accountUrl = "https://"+ STORAGE_ACCOUNT + ".blob.core.windows.net";
        DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder()
                .build();

        // test credential config
        //Mono<AccessToken> token = defaultAzureCredential.getToken(new TokenRequestContext().setScopes(Collections.singletonList("https://azdatatf.blob.core.windows.net/.default")));
        //log.info(token.block().getToken());

        BlobServiceAsyncClient client = new BlobServiceClientBuilder()
                .credential(defaultAzureCredential)
                .endpoint(accountUrl)
                .buildAsyncClient();

        BlobContainerAsyncClient blobContainerClient = client.getBlobContainerAsyncClient(CONTAINER_NAME);
        BlobAsyncClient blobAsyncClient = blobContainerClient.getBlobAsyncClient(BLOB_NAME);

        long blobSize = blobAsyncClient.downloadToFile("/Users/abhishekupman/work/a.txt").block().getBlobSize();
        log.info("download size {}", blobSize);
    }
}
