/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import com.pingcap.tikv.txn.TxnKVClient;
import com.pingcap.tikv.util.BackOffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class TiBatchWrite2PCClient {
    private static final Logger LOG = LoggerFactory.getLogger(TiBatchWrite2PCClient.class);

    private TxnKVClient kvClient;
    private long startTime;


    public TiBatchWrite2PCClient(TxnKVClient kvClient, long startTime) {
        this.kvClient = kvClient;
        this.startTime = startTime;
    }

    /**
     * 2pc - prewrite primary key
     *
     * @param backOffer
     * @param key
     * @param value
     * @return
     */
    public String prewritePrimaryKey(BackOffer backOffer, byte[] key, byte[] value) {
        // TODO
        return null;
    }

    /**
     * 2pc - prewrite secondary keys
     *
     * @param backOffer
     * @param keys
     * @param values
     * @return
     */
    public String prewriteSecondaryKeys(BackOffer backOffer, Iterator<ByteWrapper> keys, Iterator<ByteWrapper> values) {

        // TODO
        return null;
    }

    /**
     * 2pc - commit primary key
     *
     * @param backOffer
     * @param key
     * @return
     */
    public String commitPrimaryKey(BackOffer backOffer, byte[] key) {
        // TODO
        return null;
    }

    /**
     * 2pc - commit secondary keys
     *
     * @param backOffer
     * @param keys
     * @return
     */
    public String commitSecondaryKeys(BackOffer backOffer, Iterator<ByteWrapper> keys) {
        // TODO
        return null;
    }

    public static class ByteWrapper {
        private byte[] bytes;

        public ByteWrapper(byte[] bytes) {
            this.bytes = bytes;
        }

        public byte[] getBytes() {
            return this.bytes;
        }
    }
}
