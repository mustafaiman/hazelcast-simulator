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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.AttributeCreator;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.tests.map.domain.JsonSample16MBFactory;
import com.hazelcast.simulator.tests.map.domain.JsonSampleFactory;
import com.hazelcast.simulator.tests.map.domain.ObjectSampleFactory;
import com.hazelcast.simulator.tests.map.domain.SampleFactory;
import com.hazelcast.simulator.tests.map.domain.TweetJsonFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;

import java.util.Random;

public class PutGetPerfTest extends HazelcastTest {


    // properties
    public String strategy = QueryPerformanceTest.Strategy.JSON.name();
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String mapname = "default";
    public Random random = new Random();
    public String preferredSize = "";

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<Integer, Object> map;

    private Object[] values = new Object[itemCount];


    @Setup
    public void setUp() {
        SampleFactory factory;
        AttributeCreator creator = new AttributeCreator();
        if (QueryPerformanceTest.Strategy.valueOf(strategy) == QueryPerformanceTest.Strategy.JSON) {
            if (preferredSize.equals("16MB")) {
                factory = new JsonSample16MBFactory(new TweetJsonFactory(), creator);
            } else {
                factory = new JsonSampleFactory(new TweetJsonFactory(), creator);
            }
        } else {
            DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(QueryPerformanceTest.Strategy.valueOf(strategy));
            factory = new ObjectSampleFactory(objectFactory, creator);
        }
        map = targetInstance.getMap(mapname);

        for (int i = 0; i < itemCount; i++) {
            values[i] = factory.create();
            map.put(i, values[i]);
        }
    }

    @Prepare
    public void prepare() {
        throttlingLogger.info(strategy + " " + mapname + " " + useIndex + " " + itemCount);
        if (useIndex) {
            map.addIndex("stringVam", false);
        }
    }

    @TimeStep(prob = 1.0)
    public void put(BaseThreadState state) {
        int id = random.nextInt(itemCount);
        map.put(id, values[id]);
    }

    @TimeStep(prob = 0)
    public void set(BaseThreadState state) {
        int id = random.nextInt(itemCount);
        map.set(id, values[id]);
    }

    @TimeStep(prob = -1)
    public void get(BaseThreadState state) {
        map.get(random.nextInt(itemCount));
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
