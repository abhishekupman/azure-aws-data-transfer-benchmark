package com.adobe.data.tf;

import com.adobe.data.tf.model.ApplicationInput;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

public class AzureUtils {

    private final static String KEY_VAULT_NAME = "az-aws-cred";
    private final static String SECRET_AWS_ACCESS_KEY_NAME = "aws-access-key";
    private final static String SECRET_AWS_SECRET_KEY_NAME = "aws-secret-key-val";


    public static BlobAsyncClient getAzureBlobClient(ApplicationInput applicationInput) {
        final String accountUrl = "https://"+ applicationInput.getStorageAccount() + ".blob.core.windows.net";
        DefaultAzureCredential defaultAzureCredential = new DefaultAzureCredentialBuilder()
                .build();

        BlobServiceAsyncClient client = new BlobServiceClientBuilder()
                .credential(defaultAzureCredential)
                .endpoint(accountUrl)
                .buildAsyncClient();

        BlobContainerAsyncClient blobContainerClient = client.getBlobContainerAsyncClient(applicationInput.getContainerName());
        return blobContainerClient.getBlobAsyncClient(applicationInput.getBlobName());
    }

    public static AwsBasicCredentials fetchAWSCredentialsFromKeyVault() {
        String keyVaultUri = "https://" + KEY_VAULT_NAME + ".vault.azure.net";
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUri)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        KeyVaultSecret accessKeyVal = secretClient.getSecret(SECRET_AWS_ACCESS_KEY_NAME);
        KeyVaultSecret secretKeyVal = secretClient.getSecret(SECRET_AWS_SECRET_KEY_NAME);
        return AwsBasicCredentials.create(accessKeyVal.getValue(), secretKeyVal.getValue());
    }
}
