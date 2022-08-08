package com.datastax.driver.core;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collection;
import org.junit.Test;

/*@CCMConfig(startSniProxy = true, numberOfNodes = 1, config = {"native_transport_port:9042"})*/
public class WorkInProgressNameDontGitPushIntegrationTest {

  CCMBridge.Builder ccmbuilder;
  CCMBridge bridge;
  Cluster cluster;

  @Test
  public void tryParsing()
      throws InterruptedException, IOException, CertificateException, KeyStoreException,
          NoSuchAlgorithmException, InvalidKeySpecException {
    ccmbuilder = CCMBridge.builder().withNodes(1).withSniProxy().withBinaryPort(9042);
    bridge = ccmbuilder.build();
    File ccmdir = bridge.getCcmDir();
    File clusterFile = new File(ccmdir, bridge.getClusterName());
    File yamlFile = new File(clusterFile, "config_data.yaml");
    ConnectionConfig config = ConnectionConfig.fromFile(yamlFile);
    ConfigurationBundle cb = config.createBundle();
    cb.writeIdentity("/tmp/identity.jks", "cassandra".toCharArray());
    cb.writeTrustStore("/tmp/trust.jks", "cassandra".toCharArray());

    System.out.println(config.toString());

    String[] split = config.getDatacenters().get("eu-west-1").getServer().split(":");

    InetSocketAddress sniProxyAddress = InetSocketAddress.createUnresolved("127.0.1.1", Integer.parseInt(split[1]));
        //InetSocketAddress.createUnresolved(split[0], Integer.parseInt(split[1]));

    Cluster c = Cluster.builder().withTemporary(sniProxyAddress).build();
    Session s = c.connect();
    System.out.println(s.execute("SELECT broadcast_address, host_id FROM system.local").all());
    System.out.println(s.execute("SELECT peer, host_id FROM system.peers").all());
    Collection<Host> hosts = s.getState().getConnectedHosts();
    for (Host host : hosts) {
      System.out.println(host + " " + host.getBroadcastAddress());
    }
    ((SessionManager) s).cluster.manager.controlConnection.triggerReconnect();

    System.out.println(s.execute("SELECT broadcast_address, host_id FROM system.local").all());
    
    s.close();
    c.close();
  }
}
