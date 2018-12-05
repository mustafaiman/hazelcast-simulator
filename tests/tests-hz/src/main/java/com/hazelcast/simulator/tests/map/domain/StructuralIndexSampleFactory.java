package com.hazelcast.simulator.tests.map.domain;


import com.hazelcast.query.misonparser.StructuralIndex;
import com.hazelcast.query.misonparser.StructuralIndexFactory;

public class StructuralIndexSampleFactory implements SampleFactory {

    JsonSampleFactory factory;

    public StructuralIndexSampleFactory(JsonSampleFactory factory) {
        this.factory = factory;
    }

    @Override
    public StructuralIndex create() {
        return StructuralIndexFactory.create(factory.create(), 5);
    }
}
