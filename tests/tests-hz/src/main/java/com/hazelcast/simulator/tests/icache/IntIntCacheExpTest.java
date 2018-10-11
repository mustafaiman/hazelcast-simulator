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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class IntIntCacheExpTest extends HazelcastTest {
    public int expirableCacheCount = 5;
    public int notExpirableCacheCount = 5;
    public int minNumberOfMembers = 0;
    public int keyCount = 1000000;
    public int hotKeyCount = 100;
    public boolean useHotKey = true;
    public KeyLocality keyLocality = KeyLocality.SHARED;


    private List<ICache<Integer, Integer>> caches;
    private final static String EXPIRABLE_PREFIX = "expirable.";
    private int[] keys;

    @Setup
    public void setup() {
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);
        caches = new ArrayList<ICache<Integer, Integer>>();
        for (int i = 0; i < expirableCacheCount; i++) {
            caches.add(CacheUtils.<Integer, Integer>getCache(targetInstance, EXPIRABLE_PREFIX + getCacheName(i)));
        }
        for (int i = 0; i < notExpirableCacheCount; i++) {
            caches.add(CacheUtils.<Integer, Integer>getCache(targetInstance, getCacheName(i)));
        }
        Collections.shuffle(caches);
    }

    @Prepare
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        for (ICache cache : caches) {
            Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(cache);
            Random random = new Random();
            for (int key : keys) {
                int value = random.nextInt(Integer.MAX_VALUE);
                streamer.pushEntry(key, value);
            }
            streamer.await();

            logger.info("cache name=" + cache.getName() + " has size=" + cache.size());
        }
    }

    @TimeStep(prob = 0.1)
    public Integer getAndPut(ThreadState state) {
        return state.randomCache().getAndPut(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public Integer get(ThreadState state) {
        return state.randomCache().get(state.randomKey());
    }

    @TimeStep(prob = 0.2)
    public int size(ThreadState state) {
        return state.randomCache().size();
    }

    public class ThreadState extends BaseThreadState {

        private ICache<Integer, Integer> randomCache() {
            return caches.get(randomInt(notExpirableCacheCount + expirableCacheCount));
        }

        private int randomValue() {
            return randomInt();
        }

        private int randomKey() {
            return useHotKey ? keys[randomInt(hotKeyCount)]: keys[randomInt(keys.length)];
        }
    }

    private String getCacheName(int i) {
        return name + i;
    }
}
