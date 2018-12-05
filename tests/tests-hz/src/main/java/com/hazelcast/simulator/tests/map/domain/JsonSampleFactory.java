package com.hazelcast.simulator.tests.map.domain;

public class JsonSampleFactory implements SampleFactory {

    private TweetJsonFactory factory;
    private MetadataCreator creator;

    public JsonSampleFactory(TweetJsonFactory factory, MetadataCreator creator) {
        this.factory = factory;
        this.creator = creator;
    }

    @Override
    public String create() {
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return factory.buildJsonText();
    }
}
