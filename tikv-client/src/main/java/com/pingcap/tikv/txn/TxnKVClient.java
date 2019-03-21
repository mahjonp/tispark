/*
 * Copyright 2019 The TiKV Project Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.txn;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.txn.type.ClientRPCResult;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.ConcreteBackOffer;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;

/** KV client of transaction APIs for GET/PUT/DELETE/SCAN */
public class TxnKVClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TxnKVClient.class);

  private final RegionStoreClient.RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final RegionManager regionManager;
  private ReadOnlyPDClient pdClient;

  private long lockTTL = 2000;

  public RegionStoreClient.RegionStoreClientBuilder getClientBuilder() {
    return clientBuilder;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public ReadOnlyPDClient getPdClient() {
    return pdClient;
  }

  public TxnKVClient(
      TiConfiguration conf,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder,
      ReadOnlyPDClient pdClient) {
    this.conf = conf;
    this.clientBuilder = clientBuilder;
    this.regionManager = clientBuilder.getRegionManager();
    this.pdClient = pdClient;
  }

  public TiTimestamp getTimestamp() {
    BackOffer bo = ConcreteBackOffer.newTsoBackOff();
    TiTimestamp timestamp = new TiTimestamp(0, 0);
    try {
      while (true) {
        try {
          timestamp = pdClient.getTimestamp(bo);
          break;
        } catch (final TiKVException e) { // retry is exhausted
          bo.doBackOff(BackOffFunction.BackOffFuncType.BoPDRPC, e);
        }
      }
    } catch (GrpcException e1) {
      LOG.error("Get tso from pd failed,", e1);
    }
    return timestamp;
  }

  /** when encountered region error,ErrBodyMissing, and other errors */
  public ClientRPCResult prewrite(
      BackOffer backOffer,
      List<Kvrpcpb.Mutation> mutations,
      byte[] primary,
      long lockTTL,
      long startTs,
      long regionId) {
    ClientRPCResult result = new ClientRPCResult(true, false, null);
    // send request
    TiRegion region = regionManager.getRegionById(regionId);
    RegionStoreClient client = clientBuilder.build(region);
    try {
      client.prewrite(backOffer, ByteString.copyFrom(primary), mutations, startTs, lockTTL);
    } catch (final TiKVException | StatusRuntimeException e) {
      result.setSuccess(false);
      // mark retryable, region error, should retry prewrite again
      result.setRetry(e instanceof RegionException);
      result.setError(e.getMessage());
    }
    return result;
  }

  /**
   * Commit request of 2pc, add backoff logic when encountered region error, ErrBodyMissing, and
   * other errors
   *
   * @param backOffer
   * @param keys
   * @param startTs
   * @param commitTs
   * @param regionId
   * @return
   */
  public ClientRPCResult commit(
      BackOffer backOffer, byte[][] keys, long startTs, long commitTs, long regionId) {
    ClientRPCResult result = new ClientRPCResult(true, false, null);
    // send request
    TiRegion region = regionManager.getRegionById(regionId);
    RegionStoreClient client = clientBuilder.build(region);
    List<ByteString> byteList = Lists.newArrayList();
    for (byte[] key : keys) {
      byteList.add(ByteString.copyFrom(key));
    }
    try {
      client.commit(backOffer, byteList, startTs, commitTs);
    } catch (final TiKVException | StatusRuntimeException e) {
      result.setSuccess(false);
      // mark retryable, region error, should retry prewrite again
      result.setRetry(e instanceof RegionException);
      result.setError(e.getMessage());
    }
    return result;
  }

  /**
   * Request for batch rollback on TiKV, retry operation should be deal with Caller
   *
   * @param backOffer
   * @param keys
   * @param startTs
   * @param regionId
   * @return
   */
  public ClientRPCResult batchRollbackReq(
      BackOffer backOffer, byte[][] keys, long startTs, long regionId) {
    ClientRPCResult result = new ClientRPCResult(true, false, null);
    List<ByteString> byteList = Lists.newArrayList();
    for (byte[] key : keys) {
      byteList.add(ByteString.copyFrom(key));
    }
    try {
      TiRegion region = regionManager.getRegionById(regionId);
      RegionStoreClient client = clientBuilder.build(region);
      // send request
      client.batchRollback(backOffer, byteList, startTs);
    } catch (final Exception e) {
      result.setSuccess(false);
      // mark retryable, region error, should retry prewrite again
      result.setRetry(e instanceof RegionException);
      result.setError(e.getMessage());
    }
    return result;
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(1000);
  }

  public RegionManager getRegionManager() {
    return regionManager;
  }

  @Override
  public void close() throws Exception {}
}
