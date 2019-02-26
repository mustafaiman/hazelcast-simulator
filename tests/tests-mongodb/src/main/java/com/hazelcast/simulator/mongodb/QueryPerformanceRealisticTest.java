package com.hazelcast.simulator.mongodb;

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QueryPerformanceRealisticTest extends MongodbTest {

    private static final String COLLECTION_PREFIX = "c";

    public int collectionCount = 100;
    public int minEntriesPerCollection = 1;
    public int maxEntriesPerCollection = 10000;
    public String predicate = "createdAt=sancar";
    public String databaseName = "realistic";
    public long dataSizePerInstanceMB = 10;

    private String predicateLeft;
    private String predicateRight;
    private MongoCollection[] collections = new MongoCollection[collectionCount];
    private AttributeCreator attributeCreator = new AttributeCreator();
    private TweetDocumentFactory tweetDocumentFactory = new TweetDocumentFactory();
    private JsonSampleFactory jsonSampleFactory = new JsonSampleFactory(tweetDocumentFactory, attributeCreator);
    Random random = new Random();

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);

    @Setup
    public void setUp() {
        MongoDatabase database = client.getDatabase(databaseName);
        throttlingLogger.info(predicateLeft + " " + predicateRight);
        for (int i = 0; i < collectionCount; i++) {
            collections[i] = database.getCollection(COLLECTION_PREFIX + i);
            collections[i].deleteMany(new Document());
        }
        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];
    }


    @Prepare(global = false)
    public void prepare() {
        long localMB;
        for (int i = 0; i < collectionCount; i++) {
            for (int j = 0; j < minEntriesPerCollection; j++) {
                try {
                    collections[i].replaceOne(new Document("_id", j), createDocument(random.nextInt(100), j), new ReplaceOptions().upsert(true));
                } catch (MongoException e) {

                }
            }
        }
        while ((localMB = getDataCost()) < dataSizePerInstanceMB) {
            MongoCollection collection = pickNotFullCollection();
            List<Document> docs = new ArrayList<Document>();
            for (int i = 0; i < 200; i++) {
                docs.add(createDocument(random.nextInt(100), -1));
            }
            collection.insertMany(docs);
            throttlingLogger.info("Size: " + localMB);
        }
    }

    private MongoCollection pickNotFullCollection() {
        MongoCollection collection = null;
        while (collection == null) {
            collection = collections[random.nextInt(collectionCount)];
            if (collection.countDocuments() >= maxEntriesPerCollection) {
                collection = null;
            }
        }
        return collection;
    }

    private MongoCollection pickRandomCollection() {
        return collections[random.nextInt(collectionCount)];
    }

    private Document createDocument(int prob, int id) {
        if (prob < 60) {
            return jsonSampleFactory.create(id);
        } else if (prob < 85) {
            return jsonSampleFactory.create10KB(id);
        } else if (prob < 97) {
            return jsonSampleFactory.create100KB(id);
        } else {
            return jsonSampleFactory.create1000KB(id);
        }
    }

    private long getDataCost() {
        Object ob = client.getDatabase(databaseName).runCommand(new Document("dbStats", 1)).get("dataSize");
        if (ob instanceof Double) {
            return (long)(((Double)ob)/ 1024 / 1024);
        } else if (ob instanceof Long) {
            return (long)(((Long)ob)/ 1024 / 1024);
        } else if (ob instanceof Float) {
            return (long)(((Float)ob)/ 1024 / 1024);
        } else if (ob instanceof Integer) {
            return (long)(((Integer)ob)/ 1024 / 1024);
        } else {
            return 0;
        }
    }

    @TimeStep(prob = -1)
    public void get() {
        MongoCollection collection = pickRandomCollection();
        collection.find(new BasicDBObject("_id", random.nextInt(minEntriesPerCollection))).first();
    }

    @TimeStep(prob = 0.2)
    public void put() {
        int id = random.nextInt(minEntriesPerCollection);
        try {
            pickRandomCollection().replaceOne(new Document("_id", id), jsonSampleFactory.create(id), new ReplaceOptions().upsert(true));
        } catch (MongoException e) {

        }
    }

    @TimeStep(prob = 0.6)
    public void values() {
        pickRandomCollection().countDocuments(new BasicDBObject(predicateLeft, predicateRight));
    }
}
