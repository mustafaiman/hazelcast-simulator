package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.AttributeCreator;
import com.hazelcast.simulator.tests.map.domain.JsonSampleFactory;
import com.hazelcast.simulator.tests.map.domain.TweetJsonFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class QueryPerformanceRealisticTest extends HazelcastTest {

    public enum Strategy {
        PORTABLE,
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        JSON
    }

    private static String mapPrefix = "mapPrefix";

    public int mapCount = 100;
    public int minEntriesPerMap = 1;
    public int maxEntriesPerMap = 10000;
    public int minPayloadKB = 1;
    public int maxPayloadKB = 1024;
    public int totalPayloadKB = 1 * 1024 * 1024;
    public boolean useIndex = false;
    public String predicate = "createdAt=sancar";

    private String predicateLeft;
    private String predicateRight;
    private IMap[] maps = new IMap[mapCount];
    private AttributeCreator attributeCreator = new AttributeCreator();
    private TweetJsonFactory tweetJsonFactory = new TweetJsonFactory();
    private JsonSampleFactory jsonSampleFactory = new JsonSampleFactory(tweetJsonFactory, attributeCreator);

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);

    @Setup
    public void setUp() {
        Random random = new Random();
        for (int i = 0; i < mapCount; i++) {
            maps[i] = targetInstance.getMap(mapPrefix + random.nextLong());
        }
        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];
    }

    @Prepare(global = true)
    public void prepare() {
        throttlingLogger.info(useIndex + " " + predicateLeft + " " + predicateRight);
    }

    @BeforeRun
    public void beforeRun(ThreadState state) throws Exception {
        for (int i = 0; i < mapCount; i++) {
            int mapSize = state.randomInt(maxEntriesPerMap - minEntriesPerMap) + minEntriesPerMap;
            for (int j = 0; j < mapSize; j++) {
                maps[i].put(j, jsonSampleFactory.create());
                state.mapSizesMap.put(maps[i].getName(), mapSize);
            }
        }
    }

    @TimeStep(prob = 0.5)
    public void get(ThreadState state) {
        IMap map = state.randomMap();
        int mapsize = state.mapSizesMap.get(map.getName());
        map.get(state.randomInt(mapsize));
    }

    @TimeStep(prob = 0.2)
    public void put(ThreadState state) {
        IMap map = state.randomMap();
        int mapsize = state.mapSizesMap.get(map.getName());
        state.randomMap().put(state.randomInt(mapsize), jsonSampleFactory.create());
    }

    @TimeStep(prob = 0.3)
    public void values(ThreadState state) {
        state.randomMap().values(Predicates.equal(predicateLeft, predicateRight)).size();
    }

    public class ThreadState extends BaseThreadState {
        Map<String, Integer> mapSizesMap = new HashMap<String, Integer>();

        public IMap randomMap() {
            return maps[randomInt(mapCount)];
        }

        public int randomPayloadSize() {
            if (maxPayloadKB == minPayloadKB) {
                return 0;
            }
            return randomInt(maxPayloadKB - minPayloadKB) + minPayloadKB;
        }
    }

    public static void main(String[] args) {
        JsonSampleFactory f = new JsonSampleFactory(new TweetJsonFactory(), new AttributeCreator());
        System.out.println(f.create());
    }
}
