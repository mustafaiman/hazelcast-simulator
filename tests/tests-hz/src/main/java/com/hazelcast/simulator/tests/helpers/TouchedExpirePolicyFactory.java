package com.hazelcast.simulator.tests.helpers;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import java.util.concurrent.TimeUnit;

public class TouchedExpirePolicyFactory implements Factory<ExpiryPolicy> {

    private final TouchedExpiryPolicy touchedExpiryPolicy = new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 500));

    @Override
    public ExpiryPolicy create() {
        return touchedExpiryPolicy;
    }
}
