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

import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;

public class AsterixThreadFactory implements ThreadFactory {

    private final ILifeCycleComponentManager lccm;

    public AsterixThreadFactory(ILifeCycleComponentManager lifeCycleComponentManager) {
        this.lccm = lifeCycleComponentManager;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t;
        if ((r instanceof Thread)) {
            t = (Thread) r;
        } else {
            t = new Thread(r);
        }
        t.setUncaughtExceptionHandler(lccm);
        return t;
    }

}
