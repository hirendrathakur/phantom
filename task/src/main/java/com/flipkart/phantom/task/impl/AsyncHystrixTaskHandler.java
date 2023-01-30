package com.flipkart.phantom.task.impl;

import com.flipkart.phantom.task.spi.AsyncTaskHandler;
import com.flipkart.phantom.task.spi.TaskContext;
import com.flipkart.phantom.task.spi.TaskResult;

import java.util.Map;

public abstract class AsyncHystrixTaskHandler extends TaskHandler implements AsyncTaskHandler {

    @Override
    public <T, S> TaskResult<T> execute(TaskContext taskContext, String command, Map<String, Object> params, S data) throws RuntimeException {
        throw new UnsupportedOperationException("Sync execution not supported in " + this.getClass().getSimpleName());
    }

    @Override
    public String[] getCommands() {
        return new String[0];
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void shutdown(TaskContext context) throws Exception {

    }

    public <T> void releaseResources(TaskResult<T> taskResult) {
        // do nothing
    }
}
