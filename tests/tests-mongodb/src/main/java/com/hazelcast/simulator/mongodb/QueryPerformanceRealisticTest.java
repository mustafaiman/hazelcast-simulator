package com.hazelcast.simulator.mongodb;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class QueryPerformanceRealisticTest extends MongodbTest {

    public int collectionCount = 100;
    public int minEntriesPerMap = 1;
    public int maxEntriesPerMap = 10000;
    public int minPayloadKB = 1;
    public int maxPayloadKB = 1024;
    public boolean useIndex = false;
    public String predicate = "createdAt=sancar";
    public String databaseName = "test";

    private String predicateLeft;
    private String predicateRight;
    private MongoCollection[] collections = new MongoCollection[collectionCount];
    private AttributeCreator attributeCreator = new AttributeCreator();
    private TweetDocumentFactory tweetDocumentFactory = new TweetDocumentFactory();
    private JsonSampleFactory jsonSampleFactory = new JsonSampleFactory(tweetDocumentFactory, attributeCreator);
    Random random = new Random();
    Map<String, Integer> mapSizesMap = new HashMap<String, Integer>();

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);

    @Setup
    public void setUp() {
        MongoDatabase database = client.getDatabase(databaseName);

        for (int i = 0; i < collectionCount; i++) {
            collections[i] = database.getCollection("c" + random.nextLong());
        }
        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        throttlingLogger.info(useIndex + " " + predicateLeft + " " + predicateRight);
        for (int i = 0; i < collectionCount; i++) {
            int mapSize = random.nextInt(maxEntriesPerMap - minEntriesPerMap) + minEntriesPerMap;
            for (int j = 0; j < mapSize; j++) {
                collections[i].insertOne(jsonSampleFactory.create(j));
                mapSizesMap.put(collections[i].getNamespace().getCollectionName(), mapSize);
            }
        }
    }

    @BeforeRun
    public void beforeRun(BaseThreadState state) throws Exception {

    }

    @TimeStep(prob = 0.5)
    public void get(BaseThreadState state) {
        MongoCollection collection = randomCollection();
        int collectionSize = mapSizesMap.get(collection.getNamespace().getCollectionName());
        collection.find(new BasicDBObject("_id", state.randomInt(collectionSize))).first();
    }

    @TimeStep(prob = 0)
    public void put(BaseThreadState state) {
        MongoCollection collection = randomCollection();
        int mapsize = mapSizesMap.get(collection.getNamespace().getCollectionName());
        randomCollection().insertOne(jsonSampleFactory.create(state.randomInt(mapsize)));
    }

    @TimeStep(prob = 0.5)
    public void values(BaseThreadState state) {
        randomCollection().countDocuments(new BasicDBObject(predicateLeft, predicateRight));
    }


    public int randomPayloadSize() {
        if (maxPayloadKB == minPayloadKB) {
            return 0;
        }
        return random.nextInt(maxPayloadKB - minPayloadKB) + minPayloadKB;
    }
    public MongoCollection randomCollection() {
        return collections[random.nextInt(collectionCount)];
    }
}
