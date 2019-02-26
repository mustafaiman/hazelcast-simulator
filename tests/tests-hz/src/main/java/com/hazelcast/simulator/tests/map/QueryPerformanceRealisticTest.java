package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.Client;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.map.domain.AttributeCreator;
import com.hazelcast.simulator.tests.map.domain.JsonSampleFactory;
import com.hazelcast.simulator.tests.map.domain.TweetJsonFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class QueryPerformanceRealisticTest extends HazelcastTest {

    private static final String MAP_PREFIX = "mapPrefix";

    public int mapCount = 100;
    public long dataSizePerInstanceMB = 10;
    public int minEntriesPerMap = 100;
    public int maxEntriesPerMap = 1000000;
    public String predicate = "createdAt=sancar";

    private String predicateLeft;
    private String predicateRight;
    private IMap[] maps = new IMap[mapCount];
    private AttributeCreator attributeCreator = new AttributeCreator();
    private TweetJsonFactory tweetJsonFactory = new TweetJsonFactory();
    private JsonSampleFactory jsonSampleFactory = new JsonSampleFactory(tweetJsonFactory, attributeCreator);
    private Random random = new Random();

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);

    @Setup
    public void setUp() {
        throttlingLogger.info(predicateLeft + " " + predicateRight);
        for (int i = 0; i < mapCount; i++) {
            maps[i] = targetInstance.getMap(MAP_PREFIX + i);
        }
        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];
    }

    @Prepare(global = false)
    public void prepare() {
        if (targetInstance.getLocalEndpoint() instanceof Client) {
            return;
        }
        long localMB;
        for (int i = 0; i < mapCount; i++) {
            for (int j = 0; j < minEntriesPerMap; j++) {
                maps[i].put(j, createHzJsonValue(random.nextInt(100)));
            }
        }
        while ((localMB = getLocalDataCostMB(maps)) < dataSizePerInstanceMB) {
            IMap map = pickNotFullMap();
            int[] keys = generateIntKeys(200, KeyLocality.LOCAL, targetInstance);
            Streamer<Integer, HazelcastJsonValue> streamer = StreamerFactory.getInstance(map);
            for (int key : keys) {
                int dataSizeProb = random.nextInt(100);
                HazelcastJsonValue value = createHzJsonValue(dataSizeProb);

                streamer.pushEntry(key, value);
            }
            streamer.await();
            throttlingLogger.info("Size: " + localMB);
        }
    }

    private HazelcastJsonValue createHzJsonValue(int prob) {
        if (prob < 60) {
            return jsonSampleFactory.create();
        } else if (prob < 85) {
            return jsonSampleFactory.create10KB();
        } else if (prob < 97) {
            return jsonSampleFactory.create100KB();
        } else {
            return jsonSampleFactory.create1000KB();
        }
    }

    private IMap pickNotFullMap() {
        IMap map = null;
        while (map == null) {
            map = maps[random.nextInt(mapCount)];
            if (map.size() >= maxEntriesPerMap) {
                map = null;
            }
        }
        return map;
    }

    private IMap pickRandomMap() {
        return maps[random.nextInt(mapCount)];
    }

    private long getLocalDataCostMB(IMap[] maps) {
        long totalCost = 0;
        for (IMap map: maps) {
            totalCost += map.getLocalMapStats().getHeapCost();
        }
        return totalCost / 1024 / 1024;
    }

    @TimeStep(prob = -1)
    public void get() {
        pickRandomMap().get(random.nextInt(minEntriesPerMap));
    }

    @TimeStep(prob = 0.2)
    public void put() {
        pickRandomMap().put(random.nextInt(minEntriesPerMap), jsonSampleFactory.create());
    }

    @TimeStep(prob = 0.6)
    public void values() {
        pickRandomMap().values(Predicates.equal(predicateLeft, predicateRight)).size();
    }

    public static void main(String[] args) {
        JsonSampleFactory f = new JsonSampleFactory(new TweetJsonFactory(), new AttributeCreator());
        Config config = new Config();
        config.getMapConfig("default").setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE);
        config.getMapConfig("default").setBackupCount(1);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        IMap map = instance.getMap("a");
        System.out.println(map.getLocalMapStats().getHeapCost());
        map.put("1", f.create1000KB());

        System.out.println(map.getLocalMapStats().getHeapCost());
        System.out.println(f.create());
    }
}
