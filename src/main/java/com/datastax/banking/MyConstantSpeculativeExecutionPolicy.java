/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.banking;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy.SpeculativeExecutionPlan;
import com.google.common.base.Preconditions;

/**
 * A {@link SpeculativeExecutionPolicy} that schedules a given number of speculative executions, separated by a fixed delay.
 */
public class MyConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
	
	private static Logger logger = LoggerFactory.getLogger(MyConstantSpeculativeExecutionPolicy.class);
	
    private final int maxSpeculativeExecutions;
    private final long constantDelayMillis;

    /**
     * Builds a new instance.
     *
     * @param constantDelayMillis      the delay between each speculative execution. Must be strictly positive.
     * @param maxSpeculativeExecutions the number of speculative executions. Must be strictly positive.
     * @throws IllegalArgumentException if one of the arguments does not respect the preconditions above.
     */
    public MyConstantSpeculativeExecutionPolicy(final long constantDelayMillis, final int maxSpeculativeExecutions) {
        Preconditions.checkArgument(constantDelayMillis > 0,
                "delay must be strictly positive (was %d)", constantDelayMillis);
        Preconditions.checkArgument(maxSpeculativeExecutions > 0,
                "number of speculative executions must be strictly positive (was %d)", maxSpeculativeExecutions);
        this.constantDelayMillis = constantDelayMillis;
        this.maxSpeculativeExecutions = maxSpeculativeExecutions;
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return new SpeculativeExecutionPlan() {
            private final AtomicInteger remaining = new AtomicInteger(maxSpeculativeExecutions);

            @Override
            public long nextExecution(Host lastQueried) {
            	logger.info("Last Queried host : "+ lastQueried.getAddress() + " Remaining : " + remaining);
            	
                return (remaining.getAndDecrement() > 0) ? constantDelayMillis : -1;
            }
        };
    }

    @Override
    public void init(Cluster cluster) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
