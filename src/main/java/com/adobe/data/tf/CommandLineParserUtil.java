package com.adobe.data.tf;

import com.adobe.data.tf.model.ApplicationInput;
import com.azure.storage.blob.BlobUrlParts;
import org.apache.commons.cli.*;

import java.net.URI;
import java.net.URISyntaxException;

public class CommandLineParserUtil {

    public static ApplicationInput parseCommandLineArguments(String args[]) {
        Options options = new Options();
        options.addOption(getIterationOption());
        options.addOption(getChunkSizeOption());
        options.addOption(getBlobUrlOption());
        options.addOption(getS3UrlOption());
        // parse
        HelpFormatter formatter = new HelpFormatter();

        ApplicationInput applicationInput = null;
        try {
            CommandLine cmd = parseOptions(options, args);
            applicationInput = parseCommandLineInput(cmd);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("azure-aws-transfer", options);
            System.exit(1);
        }
        return applicationInput;
    }


    private static ApplicationInput parseCommandLineInput(CommandLine cmd) throws URISyntaxException {
        String blobUrl = getBlobUrl(cmd);
        BlobUrlParts blobUrlParts = BlobUrlParts.parse(blobUrl);

        URI uri = new URI(getS3Url(cmd));
        String bucketName = uri.getHost();
        String prefixKey = uri.getPath().substring(1);

        return ApplicationInput.builder()
                .storageAccount(blobUrlParts.getAccountName())
                .blobName(blobUrlParts.getBlobName())
                .containerName(blobUrlParts.getBlobContainerName())
                .s3Bucket(bucketName)
                .s3PrefixKey(prefixKey)
                .iterationCount(getInterationCount(cmd))
                .chunkSizeInMB(getChunkSizeInMB(cmd))
                .build();
    }

    private static CommandLine parseOptions(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Option getIterationOption() {
        Option option = new Option("i", "iteration", true, "Interation Count");
        option.setRequired(false);
        return option;
    }

    private static Integer getInterationCount(CommandLine cmd) {
        String iteration = cmd.getOptionValue("iteration");
        if (iteration == null) {
            return 1;
        } else {
            return Integer.parseInt(iteration);
        }
    }

    private static Option getChunkSizeOption() {
        Option option = new Option("c", "chunk-size", true, "Part Upload Chunk Size in MB");
        option.setRequired(true);
        return option;
    }

    private static Integer getChunkSizeInMB(CommandLine cmd) {
        String chunkSize = cmd.getOptionValue("chunk-size");
        return Integer.parseInt(chunkSize);
    }

    private static Option getBlobUrlOption() {
        Option option = new Option("b", "blob-url", true, "Source Blob Url");
        option.setRequired(true);
        return option;
    }

    private static String getBlobUrl(CommandLine cmd) {
        return cmd.getOptionValue("blob-url");
    }
    private static Option getS3UrlOption() {
        Option option = new Option("s", "s3-url", true, "Destination S3 Url");
        option.setRequired(true);
        return option;
    }
    private static String getS3Url(CommandLine cmd) {
        return cmd.getOptionValue("s3-url");
    }
}
