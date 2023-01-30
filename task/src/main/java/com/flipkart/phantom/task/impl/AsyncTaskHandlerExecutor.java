package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.event.ServiceProxyEvent;
import com.flipkart.phantom.task.spi.*;
import com.flipkart.phantom.task.spi.interceptor.RequestInterceptor;
import com.flipkart.phantom.task.spi.interceptor.ResponseInterceptor;
import com.github.kristofa.brave.Brave;

import com.google.common.base.Optional;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import rx.Observable;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AsyncTaskHandlerExecutor<I, O> extends HystrixObservableCommand<TaskResult<O>> implements Executor<TaskRequestWrapper<I>, TaskResult<O>> {

    public static final String NO_RESULT = "The command returned no result";
    public static final String ASYNC_QUEUED = "The command dispatched for async execution";

    /**
     * The default Hystrix group to which the command belongs, unless otherwise mentioned
     */
    public static final String DEFAULT_HYSTRIX_GROUP = "DEFAULT_GROUP";

    /**
     * The default Hystrix Thread pool to which this command belongs, unless otherwise mentioned
     */
    public static final String DEFAULT_HYSTRIX_THREAD_POOL = "DEFAULT_THREAD_POOL";

    /**
     * The default Hystrix Thread pool to which this command belongs, unless otherwise mentioned
     */
    public static final int DEFAULT_HYSTRIX_THREAD_POOL_SIZE = 10;

    /**
     * Event Type for publishing all events which are generated here
     */
    private final static String COMMAND_HANDLER = "COMMAND_HANDLER";

    /**
     * The {@link TaskHandler} or {@link HystrixTaskHandler} instance which this Command wraps around
     */
    protected AsyncHystrixTaskHandler taskHandler;

    /**
     * The params required to execute a TaskHandler
     */
    protected TaskContext taskContext;

    /**
     * The command for which this task is executed
     */
    protected String command;

    /**
     * The parameters which are utilized by the task for execution
     */
    protected Map<String, Object> params;

    /**
     * Data which is utilized by the task for execution
     */
    protected I data;

    /* Task Request Wrapper */
    protected TaskRequestWrapper<I> taskRequestWrapper;

    /* Decoder to decode requests */
    protected Decoder<O> decoder;

    /**
     * Event which records various paramenters of this request execution & published later
     */
    protected ServiceProxyEvent.Builder eventBuilder;

    /**
     * List of request and response interceptors
     */
    private List<RequestInterceptor<TaskRequestWrapper<I>>> requestInterceptors = new LinkedList<>();
    private List<ResponseInterceptor<TaskResult<O>>> responseInterceptors = new LinkedList<>();

    protected AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler, TaskContext taskContext, String commandName,
                                       TaskRequestWrapper<I> taskRequestWrapper, int concurrentRequestSize) {
        this(taskHandler, taskContext, commandName, taskRequestWrapper, concurrentRequestSize, null);
    }

    protected AsyncTaskHandlerExecutor(AsyncHystrixTaskHandler taskHandler, TaskContext taskContext, String commandName,
                                       TaskRequestWrapper<I> taskRequestWrapper, int concurrentRequestSize, Decoder<O> decoder) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(taskHandler.getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(commandName))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE).
                        withExecutionIsolationSemaphoreMaxConcurrentRequests(concurrentRequestSize)));
        this.taskHandler = taskHandler;
        this.taskContext = taskContext;
        this.command = commandName;
        this.data = taskRequestWrapper.getData();
        this.params = taskRequestWrapper.getParams();
        this.taskRequestWrapper = taskRequestWrapper;
        this.eventBuilder = new ServiceProxyEvent.Builder(commandName, COMMAND_HANDLER);
        this.decoder = decoder;

    }

    @Override
    public TaskResult<O> execute() {
        throw new UnsupportedOperationException("Sync execution is not supported in " + this.getClass().getSimpleName());
    }

    @Override
    public ServiceProxyEvent.Builder getEventBuilder() {
        return this.eventBuilder;
    }

    @Override
    public void addRequestInterceptor(RequestInterceptor<TaskRequestWrapper<I>> requestInterceptor) {
        this.requestInterceptors.add(requestInterceptor);
    }

    @Override
    public void addResponseInterceptor(ResponseInterceptor<TaskResult<O>> responseInterceptor) {
        this.responseInterceptors.add(responseInterceptor);
    }

    @Override
    public Optional<String> getServiceName() {
        return Optional.fromNullable(this.taskHandler.getName());
    }

    @Override
    public TaskRequestWrapper<I> getRequestWrapper() {
        return this.taskRequestWrapper;
    }

    @Override
    protected Observable<TaskResult<O>> construct() {
        return observableFrom(_run());
    }

    public CompletableFuture<TaskResult<O>> queue() {
        CompletableFuture<TaskResult<O>> completableFuture = new CompletableFuture<>();
        Observable<TaskResult<O>> observable = observe();
        observable.subscribe(completableFuture::complete, completableFuture::completeExceptionally);
        return completableFuture;
    }

    private CompletableFuture<TaskResult<O>> _run() {
        this.eventBuilder.withRequestExecutionStartTime(System.currentTimeMillis());
        if (this.taskRequestWrapper.getRequestContext().isPresent() && this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan() != null) {
            Brave.getServerSpanThreadBinder().setCurrentSpan(this.taskRequestWrapper.getRequestContext().get().getCurrentServerSpan());
        }
        for (RequestInterceptor<TaskRequestWrapper<I>> requestInterceptor : this.requestInterceptors) {
            requestInterceptor.process(this.taskRequestWrapper);
        }
        CompletableFuture<TaskResult<O>> result;
        if (decoder == null) {
            result = this.taskHandler.executeAsync(taskContext, command, params, data);
        } else {
            result = this.taskHandler.executeAsync(taskContext, command, taskRequestWrapper, decoder);
        }
        if (result == null) {
            result = getDefaultResult();
        }
        return result.thenApply(taskResult -> {
            if (taskResult != null && !taskResult.isSuccess()) {
                throw new RuntimeException("Command returned FALSE: " + taskResult.getMessage());
            }
            return taskResult;
        }).whenComplete((taskResult, throwable) -> {
            if (taskResult != null
                    && this.isResponseTimedOut()) {
                this.taskHandler.releaseResources(taskResult);
            }
            processResponseInterceptors(taskResult, java.util.Optional.ofNullable(throwable).map(RuntimeException::new).orElse(null));
        });
    }

    private Observable<TaskResult<O>> observableFrom(CompletableFuture<TaskResult<O>> resultFuture) {
        return Observable.create(subscriber ->
                resultFuture.whenComplete((result, error) -> {
                    if (error != null) {
                        subscriber.onError(error);
                    } else {
                        subscriber.onNext(result);
                        subscriber.onCompleted();
                    }
                }));
    }

    protected CompletableFuture<TaskResult<O>> getDefaultResult() {
        return CompletableFuture.completedFuture(new TaskResult<>(true, NO_RESULT));
    }

    void processResponseInterceptors(TaskResult<O> result, /*Nullable*/ RuntimeException exception) {
        for (ResponseInterceptor<TaskResult<O>> responseInterceptor : this.responseInterceptors) {
            responseInterceptor.process(result, Optional.fromNullable(exception));
        }
    }

}
