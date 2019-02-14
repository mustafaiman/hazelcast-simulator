package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;

import java.util.Arrays;

public class TweetJsonFactory {

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

    public String buildJsonText() {
        return build().toString();
    }

    public JsonValue build() {
        return Json.object()
                .add("createdAt", createdAt)
                .add("idStr", idStr)
                .add("text", text)
                .add("user", Json.object()
                        .add("id", id)
                        .add("name", name)
                        .add("screenName", screenName)
                        .add("location", Json.object()
                                .add("country", country)
                                .add("city", city))
                        .add("url", url)
                        .add("description", description));
    }

    public JsonValue build10KB() {
        return Json.object()
                .add("createdAt", createdAt)
                .add("idStr", idStr)
                .add("text", text)
                .add("user", Json.object()
                        .add("id", id)
                        .add("name", name)
                        .add("screenName", screenName)
                        .add("location", Json.object()
                                .add("garbage", garbage9KB)
                                .add("country", country)
                                .add("city", city))
                        .add("url", url)
                        .add("description", description));
    }

    public JsonValue build100KB() {
        return Json.object()
                .add("createdAt", createdAt)
                .add("idStr", idStr)
                .add("text", text)
                .add("garbage", garbage99KB)
                .add("user", Json.object()
                        .add("id", id)
                        .add("name", name)
                        .add("screenName", screenName)
                        .add("location", Json.object()
                                .add("country", country)
                                .add("city", city))
                        .add("url", url)
                        .add("description", description));
    }


    public JsonValue build1000KB() {
        return Json.object()
                .add("createdAt", createdAt)
                .add("idStr", idStr)
                .add("text", text)
                .add("user", Json.object()
                        .add("id", id)
                        .add("garbage", garbage999KB)
                        .add("name", name)
                        .add("screenName", screenName)
                        .add("location", Json.object()
                                .add("country", country)
                                .add("city", city))
                        .add("url", url)
                        .add("description", description));
    }
}
