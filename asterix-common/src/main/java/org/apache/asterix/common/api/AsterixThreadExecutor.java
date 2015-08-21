/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.common.api;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class AsterixThreadExecutor implements Executor {
    private final ExecutorService executorService;

    public AsterixThreadExecutor(ThreadFactory threadFactory) {
        executorService = Executors.newCachedThreadPool(threadFactory);
    }

    @Override
    public void execute(Runnable command) {
        executorService.execute(command);
    }

    public Future<Object> submit(Callable command) {
        return (Future<Object>) executorService.submit(command);
    }
}
