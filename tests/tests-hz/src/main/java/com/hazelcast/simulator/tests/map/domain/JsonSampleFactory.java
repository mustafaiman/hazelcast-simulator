package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.json.HazelcastJson;

public class JsonSampleFactory implements SampleFactory {

    private TweetJsonFactory factory;
    private AttributeCreator creator;

    public JsonSampleFactory(TweetJsonFactory factory, AttributeCreator creator) {
        this.factory = factory;
        this.creator = creator;
    }

    @Override
    public HazelcastJsonValue create() {
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return HazelcastJson.fromString(factory.buildJsonText());
    }

    public HazelcastJsonValue create10KB() {
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return HazelcastJson.fromString(factory.build10KB().toString());
    }

    public HazelcastJsonValue create100KB() {
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return HazelcastJson.fromString(factory.build100KB().toString());
    }

    public HazelcastJsonValue create1000KB() {
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return HazelcastJson.fromString(factory.build1000KB().toString());
    }
}
