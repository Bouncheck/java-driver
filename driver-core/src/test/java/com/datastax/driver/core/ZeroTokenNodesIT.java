package com.datastax.driver.core;

import static org.apache.log4j.Level.WARN;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.ScyllaVersion;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ZeroTokenNodesIT {
  private Logger logger = Logger.getLogger(ControlConnection.class);
  private MemoryAppender appender;
  private Level originalLevel;

  @DataProvider(name = "loadBalancingPolicies")
  public static Object[][] loadBalancingPolicies() {
    return new Object[][] {
      {DCAwareRoundRobinPolicy.builder().build()},
      {new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build())},
      {new TokenAwarePolicy(new RoundRobinPolicy())}
    };
  }

  @BeforeMethod(groups = "short")
  public void startCapturingLogs() {
    originalLevel = logger.getLevel();
    logger.setLevel(WARN);
    logger.addAppender(appender = new MemoryAppender());
  }

  @AfterMethod(groups = "short")
  public void stopCapturingLogs() {
    logger.setLevel(originalLevel);
    logger.removeAppender(appender);
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_ignore_zero_token_peer() {
    // Given 4 node cluster with 1 zero-token node and normal contact point,
    // make sure that it's not included in the metadata.
    // By extension, it won't be included in the query planning.
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;
    try {
      ccmBridge =
          CCMBridge.builder().withNodes(3).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setSkipZeroTokenNodes(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(1), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(1));
      String line = null;
      try {
        line = appender.waitAndGet(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      assertThat(line).contains("Found invalid row in system.peers");
      assertThat(line).contains("tokens=null");
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042", "/127.0.1.3:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_ignore_zero_token_DC() {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;
    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(2, 3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.add(2, 4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(3);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setSkipZeroTokenNodes(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(1), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(1));
      for (int i = 0; i < 2; i++) {
        String line = null;
        try {
          line = appender.waitAndGet(5000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertThat(line).contains("Found invalid row in system.peers");
        assertThat(line).contains("tokens=null");
      }
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short", dataProvider = "loadBalancingPolicies")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_connect_to_zero_token_contact_point(LoadBalancingPolicy loadBalancingPolicy) {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;

    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.waitForUp(3);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setSkipZeroTokenNodes(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(3), 9042))
              .withPort(9042)
              .withLoadBalancingPolicy(loadBalancingPolicy)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(3));
      String line = null;
      try {
        line = appender.waitAndGet(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      assertThat(line)
          .contains(
              "is a zero-token node. It will not be added to metadata hosts and will be excluded from regular query planning.");
      assertThat(line).contains(ccmBridge.ipOfNode(3));

      session.execute("DROP KEYSPACE IF EXISTS ZeroTokenNodesIT");
      session.execute(
          "CREATE KEYSPACE ZeroTokenNodesIT WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};");
      session.execute("CREATE TABLE ZeroTokenNodesIT.tab (id INT PRIMARY KEY)");
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("INSERT INTO ZeroTokenNodesIT.tab (id) VALUES (" + i + ")");
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isNotEqualTo(ccmBridge.addressOfNode(3));
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isIn(ccmBridge.addressOfNode(1), ccmBridge.addressOfNode(2));
      }
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short", dataProvider = "loadBalancingPolicies")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_connect_to_zero_token_DC(LoadBalancingPolicy loadBalancingPolicy) {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;

    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(2, 3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.add(2, 4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(3);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setSkipZeroTokenNodes(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(3), 9042))
              .withPort(9042)
              .withLoadBalancingPolicy(loadBalancingPolicy)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(3));
      String line = null;
      try {
        line = appender.waitAndGet(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      assertThat(line)
          .contains(
              "is a zero-token node. It will not be added to metadata hosts and will be excluded from regular query planning.");
      assertThat(line).contains(ccmBridge.ipOfNode(3));

      session.execute("DROP KEYSPACE IF EXISTS ZeroTokenNodesIT");
      session.execute(
          "CREATE KEYSPACE ZeroTokenNodesIT WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2};");
      session.execute("CREATE TABLE ZeroTokenNodesIT.tab (id INT PRIMARY KEY)");
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("INSERT INTO ZeroTokenNodesIT.tab (id) VALUES (" + i + ")");
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isNotEqualTo(ccmBridge.addressOfNode(3));
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isIn(ccmBridge.addressOfNode(1), ccmBridge.addressOfNode(2));
      }
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }
}
