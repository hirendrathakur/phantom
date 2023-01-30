package com.flipkart.phantom.task.spi;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface AsyncTaskHandler {

    <T, S> CompletableFuture<TaskResult<T>> executeAsync(TaskContext taskContext,
                                                    String command,
                                                    Map<String, Object> params,
                                                    S data) throws RuntimeException;

    <T, S> CompletableFuture<TaskResult<T>> executeAsync(TaskContext taskContext,
                                                    String command,
                                                    TaskRequestWrapper<S> taskRequestWrapper,
                                                    Decoder<T> decoder) throws RuntimeException;
}
