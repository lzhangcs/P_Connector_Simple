/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.fizzbuzz;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.HashMap;


import static com.facebook.presto.fizzbuzz.ExampleTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;


public class FizzBuzzConnector
        implements Connector {
    private static final Logger log = Logger.get(FizzBuzzConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ExampleMetadata metadata;
    private final FizzBuzzSplitManager splitManager;
    private final FizzBuzzRecordSetProvider recordSetProvider;


    //linda
    //This func. is used to generate requested Query data
    public HashMap<Integer, String> genhashmap(String choice) {
        int N = 10000;

        HashMap<Integer, String> fullmap = new HashMap<Integer, String>();
        HashMap<Integer, String> fizzmap = new HashMap<Integer, String>();
        HashMap<Integer, String> buzzmap = new HashMap<Integer, String>();
        HashMap<Integer, String> fizzbuzzmap = new HashMap<Integer, String>();

        for (int i = 1; i <= N; i++) {
            if (i % 3 == 0 && i % 5 == 0) {
                fullmap.put(i, "FizzBuzz");
                fizzbuzzmap.put(i, "FizzBuzz");
            } else if (i % 3 == 0) {
                fullmap.put(i, "Fizz");
                fizzmap.put(i, "Fizz");
            } else if (i % 5 == 0) {
                fullmap.put(i, "Buzz");
                buzzmap.put(i, "Buzz");
            } else
                fullmap.put(i, Integer.toString(i));
        }

        //linda


        @Override
        public ConnectorTransactionHandle beginTransaction (IsolationLevel isolationLevel,boolean readOnly)
        {
            return INSTANCE;
        }

        @Override
        public ConnectorMetadata getMetadata (ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager ()
        {
            return splitManager;
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider ()
        {
            return recordSetProvider;
        }

        @Override
        public final void shutdown ()
        {
            try {
                lifeCycleManager.stop();
            } catch (Exception e) {
                log.error(e, "Error shutting down connector");
            }
        }
    }
}
