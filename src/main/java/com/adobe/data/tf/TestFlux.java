package com.adobe.data.tf;

import com.adobe.data.tf.model.PartUploadContext;
import com.azure.storage.blob.implementation.util.ChunkedDownloadUtils;
import com.azure.storage.blob.models.BlobRange;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Random;

@Slf4j
public class TestFlux {

    public static void main(String[] args) {
        Mono<String> stringMono = Mono.fromCallable(() -> {
            log.info("sleeping for 2 sec");
            Thread.sleep(2000);
            log.info("sleeping done 2 sec");
            return "first";
        }).doOnSubscribe(a -> log.info("starting string Mono"))
                .doOnRequest(a -> log.info("starting string Mono -- Request"))
          .doOnNext(a -> log.info("done string Mono"))
          .subscribeOn(Schedulers.boundedElastic());

        Mono<Long> longMono1 = Mono.fromCallable(() -> {
            log.info("sleeping for 5 sec");
            Thread.sleep(5000);
            log.info("sleeping done 5 sec");
            return 1L;
        }).doOnSubscribe(a -> log.info("starting long Mono"))
                .doOnRequest(a -> log.info("starting long Mono -- Request"))
           .doOnNext(a -> log.info("done long Mono"))
           .subscribeOn(Schedulers.boundedElastic());

        String block = stringMono.zipWith(longMono1, (str, lg) -> str + "..." + lg)//.subscribeOn(Schedulers.boundedElastic())
                .log()
                .block();

    }


    public static void main1(String[] args) {
        Sinks.One<String> sink = Sinks.one();
        long blobSize = 33 * 1024 * 1024;
        long chunkSize = 10 * 1024 * 1024;
        int chunkCount = ChunkedDownloadUtils.calculateNumBlocks(blobSize, chunkSize);
        log.info("chunk count {}", chunkCount);
        Flux.range(0, chunkCount).
                //subscribeOn(Schedulers.parallel()).
                map(num -> {
                    Long offset = num * chunkSize;
                    Long byteCount = Math.min(chunkSize, blobSize - offset);
                    BlobRange blobRange = new BlobRange(offset, byteCount);
                    return new PartUploadContext(num+1,  null, blobRange);
                }).
                flatMap(context -> processSomething(context).subscribeOn(Schedulers.boundedElastic())).
                log().
                collectList().subscribe(list -> {
                    sink.tryEmitValue("done");
                });
        log.info("waiting for main to done");
        sink.asMono().block();
    }

    private static Mono<PartUploadContext> processSomething(PartUploadContext context) {
        return Mono.fromCallable(() -> {
            Random random =  new Random();
            int val = random.nextInt(10 - 1 + 1) + 1;
            log.info("sleeping for {} sec", val);
            Thread.sleep(val*1000);
            log.info("sleep done");
            return context;
        });
    }
}
