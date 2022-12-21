package com.adobe.data.tf;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class TestCompletableFuture {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        List<Pair<Integer>> list = Flux.range(0, 10).
                flatMap(val -> Mono.fromCompletionStage(captureTime(doSomeThing(val)))).
                collectList().
                block();

        long end = System.currentTimeMillis();
        log.info("total time {} ", (end -start));
        log.info("final list {}", list);
    }

    private static <T> CompletableFuture<Pair<T>> captureTime(CompletableFuture<T> testFuture) {
        long start = System.currentTimeMillis();
        CompletableFuture<Pair<T>> completableFuture = testFuture.thenApply(t -> new Pair(t, System.currentTimeMillis() - start));
        return completableFuture;
    }


    private static CompletableFuture<Integer> doSomeThing(int time) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Sleeping for sec {} ",time);
            sleep(time);
            log.info("Slept for sec {} ",time);
            return time;
        });
    }

    private static void sleep(int sec) {
        try {
            Thread.sleep(sec*1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ToString
    @AllArgsConstructor
    static class Pair<T> {
        T value;
        long y;
    }
}
