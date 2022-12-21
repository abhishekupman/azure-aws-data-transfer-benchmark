package com.adobe.data.tf.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public class FinalUploadResult {
    String blobUrl;
    long fileSizeInBytes;
    List<PartUploadResult> partUploadResults;
    long totalTimeTakenInMillSec;
    long partsUploadTimeTakenInMillSec;
    long initUploadApiCallTimeInMillSec;
    long completeMultiPartApiCallTimeInMillSec;
}
