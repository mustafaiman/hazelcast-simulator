package com.hazelcast.simulator.mongodb;

import org.bson.Document;

public class JsonSample16MBFactory extends JsonSampleFactory {

    private static final String FIFTEEN_MB_GARBAGE;
    private static final String FIVE_KB_GARBAGE = "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";

    static {
        StringBuilder builder = new StringBuilder(1024*1024*15);
        for (int i = 0; i < 1024 * 3; i++) {
            builder.append(FIVE_KB_GARBAGE);
        }
        FIFTEEN_MB_GARBAGE = builder.toString();
    }

    private TweetDocumentFactory factory;
    private AttributeCreator creator;

    public JsonSample16MBFactory(TweetDocumentFactory factory, AttributeCreator creator) {
        super(factory, creator);
        this.factory = factory;
        this.creator = creator;
    }

    @Override
    public Document create(int recordId) {
        factory.setRecordId(recordId);
        factory.setUrl(creator.getUrl());
        factory.setText(FIFTEEN_MB_GARBAGE);
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