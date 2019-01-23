package com.hazelcast.simulator.mongodb;

import org.bson.Document;

public class TweetDocumentFactory {

    protected String createdAt;
    protected String idStr;
    protected String text;

    // <user>
    protected int id;
    protected String name;
    protected String screenName;

    // <location>
    protected String country;
    protected String city;
    // </location>

    protected String url;
    protected String description;
    // </user>

    protected int recordId;

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public void setIdStr(String idStr) {
        this.idStr = idStr;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }

    public String buildJsonText() {
        return build().toString();
    }

    public Document build() {
        return new Document()
                .append("_id", recordId)
                .append("createdAt", createdAt)
                .append("idStr", idStr)
                .append("text", text)
                .append("user", new Document()
                        .append("id", id)
                        .append("name", name)
                        .append("screenName", screenName)
                        .append("location", new Document()
                                .append("country", country)
                                .append("city", city))
                        .append("url", url)
                        .append("description", description));
    }
}
