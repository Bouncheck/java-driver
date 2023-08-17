/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.osgi.support;

import static com.datastax.oss.driver.internal.osgi.support.CcmStagedReactor.CCM_BRIDGE;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.requirement.VersionRequirement;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.ops4j.pax.exam.junit.PaxExam;

public class CcmPaxExam extends PaxExam {

  public CcmPaxExam(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  public void run(RunNotifier notifier) {
    Description description = getDescription();
    BackendType backend =
        CCM_BRIDGE.getDseVersion().isPresent() ? BackendType.DSE : BackendType.CASSANDRA;
    Version version = CCM_BRIDGE.getDseVersion().orElseGet(CCM_BRIDGE::getCassandraVersion);

    Collection<VersionRequirement> requirements =
        VersionRequirement.fromAnnotations(getDescription());
    if (!VersionRequirement.meetsAny(requirements, backend, version)) {
      // requirements not met, throw reasoning assumption to skip test
      AssumptionViolatedException e =
          new AssumptionViolatedException(
              VersionRequirement.buildReasonString(requirements, backend, version));
      notifier.fireTestAssumptionFailed(new Failure(description, e));
    }

    // Legacy skipping:
    // TODO: use VersionRequirement

    CassandraRequirement cassandraRequirement =
        description.getAnnotation(CassandraRequirement.class);
    if (cassandraRequirement != null) {
      if (!cassandraRequirement.min().isEmpty()) {
        Version minVersion = Objects.requireNonNull(Version.parse(cassandraRequirement.min()));
        if (minVersion.compareTo(CCM_BRIDGE.getCassandraVersion()) > 0) {
          fireRequirementsNotMet(notifier, description, cassandraRequirement.min(), false, false);
          return;
        }
      }
      if (!cassandraRequirement.max().isEmpty()) {
        Version maxVersion = Objects.requireNonNull(Version.parse(cassandraRequirement.max()));
        if (maxVersion.compareTo(CCM_BRIDGE.getCassandraVersion()) <= 0) {
          fireRequirementsNotMet(notifier, description, cassandraRequirement.max(), true, false);
          return;
        }
      }
    }
    DseRequirement dseRequirement = description.getAnnotation(DseRequirement.class);
    if (dseRequirement != null) {
      Optional<Version> dseVersionOption = CCM_BRIDGE.getDseVersion();
      if (!dseVersionOption.isPresent()) {
        notifier.fireTestAssumptionFailed(
            new Failure(
                description,
                new AssumptionViolatedException("Test Requires DSE but C* is configured.")));
        return;
      } else {
        Version dseVersion = dseVersionOption.get();
        if (!dseRequirement.min().isEmpty()) {
          Version minVersion = Objects.requireNonNull(Version.parse(dseRequirement.min()));
          if (minVersion.compareTo(dseVersion) > 0) {
            fireRequirementsNotMet(notifier, description, dseRequirement.min(), false, true);
            return;
          }
        }
        if (!dseRequirement.max().isEmpty()) {
          Version maxVersion = Objects.requireNonNull(Version.parse(dseRequirement.max()));
          if (maxVersion.compareTo(dseVersion) <= 0) {
            fireRequirementsNotMet(notifier, description, dseRequirement.min(), true, true);
            return;
          }
        }
      }
    }
    ScyllaRequirement scyllaRequirement = description.getAnnotation(ScyllaRequirement.class);
    if (scyllaRequirement != null) {
      Optional<Version> scyllaVersionOption = CCM_BRIDGE.getScyllaVersion();
      if (!scyllaVersionOption.isPresent()) {
        notifier.fireTestAssumptionFailed(
            new Failure(
                description,
                new AssumptionViolatedException("Test Requires Scylla but it is not configured.")));
        return;
      }
      Version scyllaVersion = scyllaVersionOption.get();
      if (CcmBridge.SCYLLA_ENTERPRISE) {
        if (!scyllaRequirement.minEnterprise().isEmpty()) {
          Version minVersion =
              Objects.requireNonNull(Version.parse(scyllaRequirement.minEnterprise()));
          if (minVersion.compareTo(scyllaVersion) > 0) {
            fireRequirementsNotMet(
                notifier, description, scyllaRequirement.minEnterprise(), false, false);
            return;
          }
        }
        if (!scyllaRequirement.maxEnterprise().isEmpty()) {
          Version maxVersion =
              Objects.requireNonNull(Version.parse(scyllaRequirement.maxEnterprise()));
          if (maxVersion.compareTo(scyllaVersion) <= 0) {
            fireRequirementsNotMet(
                notifier, description, scyllaRequirement.maxEnterprise(), true, false);
            return;
          }
        }
      } else {
        if (!scyllaRequirement.minOSS().isEmpty()) {
          Version minVersion = Objects.requireNonNull(Version.parse(scyllaRequirement.minOSS()));
          if (minVersion.compareTo(scyllaVersion) > 0) {
            fireRequirementsNotMet(notifier, description, scyllaRequirement.minOSS(), false, false);
            return;
          }
        }
        if (!scyllaRequirement.maxOSS().isEmpty()) {
          Version maxVersion = Objects.requireNonNull(Version.parse(scyllaRequirement.maxOSS()));
          if (maxVersion.compareTo(CcmBridge.VERSION) <= 0) {
            fireRequirementsNotMet(notifier, description, scyllaRequirement.maxOSS(), true, false);
            return;
          }
        }
      }
    }
    super.run(notifier);
  }

  private void fireRequirementsNotMet(
      RunNotifier notifier,
      Description description,
      String requirement,
      boolean lessThan,
      boolean dse) {
    AssumptionViolatedException e =
        new AssumptionViolatedException(
            String.format(
                "Test requires %s %s %s but %s is configured.  Description: %s",
                lessThan ? "less than" : "at least",
                dse ? "DSE" : (CcmBridge.SCYLLA_ENABLEMENT ? "SCYLLA" : "C*"),
                requirement,
                dse
                    ? CCM_BRIDGE.getDseVersion().orElse(null)
                    : (CcmBridge.SCYLLA_ENABLEMENT
                        ? CCM_BRIDGE.getScyllaVersion().orElse(null)
                        : CCM_BRIDGE.getCassandraVersion()),
                description));
    notifier.fireTestAssumptionFailed(new Failure(description, e));
  }
}
