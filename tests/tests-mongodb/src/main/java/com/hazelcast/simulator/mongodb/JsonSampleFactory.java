package com.hazelcast.simulator.mongodb;

import org.bson.Document;

public class JsonSampleFactory {

    private TweetDocumentFactory factory;
    private AttributeCreator creator;

    public JsonSampleFactory(TweetDocumentFactory factory, AttributeCreator creator) {
        this.factory = factory;
        this.creator = creator;
    }

    public Document create(int recordId) {
        factory.setRecordId(recordId);
        factory.setUrl(creator.getUrl());
        factory.setText(creator.getText());
        factory.setScreenName(creator.getScreenName());
        factory.setName(creator.getName());
        factory.setIdStr(creator.getIdStr());
        factory.setDescription(creator.getDescription());
        factory.setCreatedAt(creator.getCreatedAt());
        factory.setCity(creator.getCity());
        factory.setCountry(creator.getCountry());
        return factory.build();
    }
}
