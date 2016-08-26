/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.protocol.operation;

public class RcKillWorkersOperation implements SimulatorOperation {
    private final int count;
    private final String versionSpec;
    private final String workerType;

    public RcKillWorkersOperation(int count, String versionSpec, String workerType) {
        this.count = count;
        this.versionSpec = versionSpec;
        this.workerType = workerType;
    }

    public int getCount() {
        return count;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public String getWorkerType() {
        return workerType;
    }
}
