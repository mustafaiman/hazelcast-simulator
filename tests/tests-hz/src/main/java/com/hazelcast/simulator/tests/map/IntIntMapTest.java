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
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class IntIntMapTest extends HazelcastTest {
    // properties
    public int keyCount = 1000000;
    public int expirableMapCount = 5;
    public int minNumberOfMembers = 0;
    public int notExpirableMapCount = 5;
    public String expirableMapPrefix = "expirable.";
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private int[] keys;
    private List<IMap<Integer,Integer>> maps;

    @Setup
    public void setUp() {
        maps = new ArrayList<IMap<Integer,Integer>>();

        for (int i = 0; i < expirableMapCount; i++) {
            maps.add(targetInstance.<Integer, Integer>getMap(expirableMapPrefix + name + i));
        }

        for (int i = 0; i < notExpirableMapCount; i++) {
            maps.add(targetInstance.<Integer, Integer>getMap(notExpirableMapCount + name + i));
        }
    }

    @Prepare(global = false)
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        for (IMap map : maps) {
            Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
            Random random = new Random();
            for (int key : keys) {
                int value = random.nextInt(Integer.MAX_VALUE);
                streamer.pushEntry(key, value);
            }
            streamer.await();
        }
    }

    IMap<Integer,Integer> getRandomMap(ThreadState state) {
        return maps.get(state.randomInt(expirableMapCount + notExpirableMapCount));
    }

    @TimeStep(prob = 0.1)
    public Integer keySet(ThreadState state) {
        Set<Integer> keySet = getRandomMap(state).keySet();
        return keySet.size();
    }

    @TimeStep(prob = 0.1)
    public Integer entrySet(ThreadState state) {
        Set<Map.Entry<Integer, Integer>> entrySet = getRandomMap(state).entrySet();
        return entrySet.size();
    }

    @TimeStep(prob = 0.1)
    public Integer put(ThreadState state) {
        int key = state.randomKey();
        int value = state.randomValue();
        return getRandomMap(state).put(key, value);
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        int key = state.randomKey();
        int value = state.randomValue();
        getRandomMap(state).set(key, value);
    }

    @TimeStep(prob = -1)
    public Integer get(ThreadState state) {
        int key = state.randomKey();
        return getRandomMap(state).get(key);
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }
}
