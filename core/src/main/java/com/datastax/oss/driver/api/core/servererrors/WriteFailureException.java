/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.util.Map;

/**
 * A non-timeout error during a write query.
 *
 * <p>This happens when some of the replicas that were contacted by the coordinator replied with an
 * error.
 *
 * <p>This exception is processed by {@link RetryPolicy#onErrorResponseVerdict(Request,
 * CoordinatorException, int)}, which will decide if it is rethrown directly to the client or if the
 * request should be retried. If all other tried nodes also fail, this exception will appear in the
 * {@link AllNodesFailedException} thrown to the client.
 */
public class WriteFailureException extends QueryConsistencyException {

  private final WriteType writeType;
  private final int numFailures;
  private final Map<InetAddress, Integer> reasonMap;

  public WriteFailureException(
      @NonNull Node coordinator,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      @NonNull WriteType writeType,
      int numFailures,
      @NonNull Map<InetAddress, Integer> reasonMap) {
    this(
        coordinator,
        String.format(
            "Cassandra failure during write query at consistency %s "
                + "(%d responses were required but only %d replica responded, %d failed)",
            consistencyLevel, blockFor, received, numFailures),
        consistencyLevel,
        received,
        blockFor,
        writeType,
        numFailures,
        reasonMap,
        null,
        false);
  }

  private WriteFailureException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      @NonNull WriteType writeType,
      int numFailures,
      @NonNull Map<InetAddress, Integer> reasonMap,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(
        coordinator,
        message,
        consistencyLevel,
        received,
        blockFor,
        executionInfo,
        writableStackTrace);
    this.writeType = writeType;
    this.numFailures = numFailures;
    this.reasonMap = reasonMap;
  }

  /** The type of the write for which this failure was raised. */
  @NonNull
  public WriteType getWriteType() {
    return writeType;
  }

  /** Returns the number of replicas that experienced a failure while executing the request. */
  public int getNumFailures() {
    return numFailures;
  }

  /**
   * Returns the a failure reason code for each node that failed.
   *
   * <p>At the time of writing, the existing reason codes are:
   *
   * <ul>
   *   <li>{@code 0x0000}: the error does not have a specific code assigned yet, or the cause is
   *       unknown.
   *   <li>{@code 0x0001}: The read operation scanned too many tombstones (as defined by {@code
   *       tombstone_failure_threshold} in {@code cassandra.yaml}, causing a {@code
   *       TombstoneOverwhelmingException}.
   * </ul>
   *
   * (please refer to the Cassandra documentation for your version for the most up-to-date list of
   * errors)
   *
   * <p>This feature is available for protocol v5 or above only. With lower protocol versions, the
   * map will always be empty.
   */
  @NonNull
  public Map<InetAddress, Integer> getReasonMap() {
    return reasonMap;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new WriteFailureException(
        getCoordinator(),
        getMessage(),
        getConsistencyLevel(),
        getReceived(),
        getBlockFor(),
        writeType,
        numFailures,
        reasonMap,
        getExecutionInfo(),
        true);
  }
}
