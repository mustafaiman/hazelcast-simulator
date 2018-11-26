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
package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableDomainObject extends AbstractDomainObject implements Portable {

    static final int CLASS_ID = 1;

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return PortableObjectFactory.FACTORY_ID;
    }

    @Override
    public void writePortable(PortableWriter out) throws IOException {
        out.writeUTF("key", key);
        out.writeUTF("stringVam", stringVam);
        out.writeDouble("doubleVal", doubleVal);
        out.writeLong("longVal", longVal);
        out.writeInt("intVal", intVal);
        out.writeUTF("longStringField", longStringField);
    }

    @Override
    public void readPortable(PortableReader in) throws IOException {
        key = in.readUTF("key");
        stringVam = in.readUTF("stringVam");
        doubleVal = in.readDouble("doubleVal");
        longVal = in.readLong("longVal");
        intVal = in.readInt("intVal");
        longStringField = in.readUTF("longStringField");
    }
}
