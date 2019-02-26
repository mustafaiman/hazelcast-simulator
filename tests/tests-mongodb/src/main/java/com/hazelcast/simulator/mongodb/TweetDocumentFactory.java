package com.hazelcast.simulator.mongodb;

import org.bson.Document;

import java.util.Arrays;

public class TweetDocumentFactory {

    private static final String garbage9KB;
    private static final String garbage99KB;
    private static final String garbage999KB;

    static {
        char[] nineKB = new char[9 * 1024];
        Arrays.fill(nineKB, 'a');
        garbage9KB = new String(nineKB);

        char[] ninetynineKB = new char[99 * 1024];
        Arrays.fill(ninetynineKB, 'a');
        garbage99KB = new String(ninetynineKB);

        char[] nnnKB = new char[999 * 1024];
        Arrays.fill(nnnKB, 'a');
        garbage999KB = new String(nnnKB);
    }

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

    protected String gibberish = "";

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
        Document doc = new Document();
        if (recordId > -1) {
            doc.append("_id", recordId);
        }
        doc.append("gibberish", gibberish)
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
        return doc;
    }

    public Document build10KB() {
        Document doc = new Document();
        if (recordId > -1) {
            doc.append("_id", recordId);
        }
        doc
                .append("gibberish", gibberish)
                .append("createdAt", createdAt)
                .append("idStr", idStr)
                .append("text", text)
                .append("user", new Document()
                        .append("id", id)
                        .append("name", name)
                        .append("screenName", screenName)
                        .append("location", new Document()
                                .append("garbage", garbage9KB)
                                .append("country", country)
                                .append("city", city))
                        .append("url", url)
                        .append("description", description));
        return doc;
    }

    public Document build100KB() {
        Document doc = new Document();
        if (recordId > -1) {
            doc.append("_id", recordId);
        }
        doc
                .append("gibberish", gibberish)
                .append("createdAt", createdAt)
                .append("idStr", idStr)
                .append("text", text)
                .append("garbage", garbage99KB)
                .append("user", new Document()
                        .append("id", id)
                        .append("name", name)
                        .append("screenName", screenName)
                        .append("location", new Document()
                                .append("country", country)
                                .append("city", city))
                        .append("url", url)
                        .append("description", description));
        return doc;
    }

    public Document build1000KB() {
        Document doc = new Document();
        if (recordId > -1) {
            doc.append("_id", recordId);
        }
        doc
                .append("gibberish", gibberish)
                .append("createdAt", createdAt)
                .append("idStr", idStr)
                .append("text", text)
                .append("user", new Document()
                        .append("id", id)
                        .append("garbage", garbage999KB)
                        .append("name", name)
                        .append("screenName", screenName)
                        .append("location", new Document()
                                .append("country", country)
                                .append("city", city))
                        .append("url", url)
                        .append("description", description));
        return doc;
    }
}
