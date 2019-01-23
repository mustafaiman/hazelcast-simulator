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
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;

import java.util.HashSet;
import java.util.Set;


public class QueryPerformanceMongoTest extends MongodbTest {

    // properties
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String predicate = "createdAt=sancar";
    public String databaseName = "test";
    public String collectionName = "queryingTest";

    private String predicateLeft;
    private String predicateRight;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private MongoCollection<Document> collection;
    private Set<String> uniqueStrings;

    @Setup
    public void setUp() {
        MongoDatabase database = client.getDatabase(databaseName);
        collection = database.getCollection(collectionName);


        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];
    }

    @Prepare(global = true)
    public void prepare() {
        throttlingLogger.info(collectionName + " " + useIndex + " " + itemCount + " " + predicateLeft + " " + predicateRight);
        if (useIndex) {
            collection.createIndex(Indexes.ascending(predicateLeft));
        }

        AttributeCreator attributeCreator = new AttributeCreator();
        TweetDocumentFactory tweetFactory = new TweetDocumentFactory();
        JsonSampleFactory sampleFactory = new JsonSampleFactory(tweetFactory, attributeCreator);


        for (int i = 0; i < itemCount; i++) {
            collection.insertOne(sampleFactory.create(i));
        }
    }

    private String[] generateUniqueStrings(int uniqueStringsCount) {
        Set<String> stringsSet = new HashSet<String>(uniqueStringsCount);
        do {
            String randomString = RandomStringUtils.randomAlphabetic(30);
            stringsSet.add(randomString);
        } while (stringsSet.size() != uniqueStringsCount);
        uniqueStrings.addAll(stringsSet);
        return stringsSet.toArray(new String[uniqueStringsCount]);
    }

    @BeforeRun
    public void beforeRun(BaseThreadState state) throws Exception {
//        state.localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
    }

    @TimeStep(prob = 1)
    public void getByStringIndex(BaseThreadState state) {
        long foundDocs = collection.countDocuments(new BasicDBObject(predicateLeft, predicateRight));
    }

    @Teardown
    public void tearDown() {
        collection.drop();
    }

    private static final Block<Document> nothingBlock = new Block<Document>() {
        @Override
        public void apply(Document document) {
            if (document != null) {
                System.out.println(document.toString());
            } else {
                System.out.println("a");
            }
        }
    };
}
