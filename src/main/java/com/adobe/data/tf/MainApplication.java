package com.adobe.data.tf;

import com.adobe.data.tf.model.ApplicationInput;
import com.adobe.data.tf.model.FinalUploadResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MainApplication {

    public static void main(String[] args) throws JsonProcessingException {
        ApplicationInput applicationInput = CommandLineParserUtil.parseCommandLineArguments(args);
        log.info("user input {}", applicationInput);
        long start = System.currentTimeMillis();
        List<FinalUploadResult> result = MultipartFileUpload.testMultiPartUploadToS3(applicationInput);
        long end = System.currentTimeMillis();
        log.info("time taken to complete all iteration {} ms", (end-start));

        log.info("outputting result");
        log.info("..........................................");
        System.out.println(new ObjectMapper().writeValueAsString(result));
    }
}
