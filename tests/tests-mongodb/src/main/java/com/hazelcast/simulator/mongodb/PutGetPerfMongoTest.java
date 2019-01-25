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
package com.hazelcast.simulator.mongodb;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.util.Random;

public class PutGetPerfMongoTest extends MongodbTest {

    // properties
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String databaseName = "test";
    public String collectionName = "readWriteTest";
    public String preferredSize = "";

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private MongoCollection<Document> col;
    private Random random = new Random();

    private Document[] values = new Document[itemCount];

    @Setup
    public void setUp() {
        if (itemCount <= 0) {
            throw new IllegalStateException("itemCount must be larger than 0");
        }

        MongoDatabase database = client.getDatabase(databaseName);
        col = database.getCollection(collectionName);

        for (int i = 0; i < itemCount; i++) {
            values[i] = createObject(i);
            col.insertOne(values[i]);
        }
    }

    @Prepare
    public void prepare() {
        throttlingLogger.info(databaseName + " " + collectionName + " " + useIndex + " " + itemCount + " " + preferredSize);
        if (useIndex) {
            col.createIndex(Indexes.ascending("_id"));
        }

    }

    @TimeStep(prob = 1.0)
    public void put(BaseThreadState state) {
        int id = random.nextInt(itemCount);
        col.replaceOne(Filters.eq("_id", id), values[id]);
    }

    @TimeStep(prob = -1)
    public void get(BaseThreadState state) {
        int id = random.nextInt(itemCount);
        col.find(Filters.eq("_id", id)).first();
    }

    @Teardown
    public void tearDown() {
        col.drop();
    }

    private Document createObject(int entryId) {
        AttributeCreator attributeCreator = new AttributeCreator();
        TweetDocumentFactory documentFactory = new TweetDocumentFactory();
        JsonSampleFactory factory = null;
        if (preferredSize.equals("16MB")) {
            factory = new JsonSample16MBFactory(documentFactory, attributeCreator);
        } else {
            factory = new JsonSampleFactory(documentFactory, attributeCreator);

        }
        return factory.create(entryId);
    }
}
