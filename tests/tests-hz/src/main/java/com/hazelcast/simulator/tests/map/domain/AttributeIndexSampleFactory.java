package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.query.json.AttributeIndex;

public class AttributeIndexSampleFactory implements SampleFactory {

    JsonSampleFactory factory;

    public AttributeIndexSampleFactory(JsonSampleFactory factory) {
        this.factory = factory;
    }

    @Override
    public AttributeIndex create() {
        return AttributeIndex.create(factory.create());
    }
}
