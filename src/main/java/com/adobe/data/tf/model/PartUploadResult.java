package com.adobe.data.tf.model;

import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class PartUploadResult {
    long partSizeInBytes;
    long timeTakenInMillSec;
}
