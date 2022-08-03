package com.datastax.driver.core;

import java.io.File;
import java.util.Collection;

public class Test {
  public static void main(String[] args) {
    Cluster c =
        Cluster.builder()
            .withCloudSecureConnectBundle(new File("../smieci/scaas-example/gocql/cmd/config.zip"))
            .build();
    Session s = c.connect();
    System.out.println(s.execute("SELECT broadcast_address, host_id FROM system.local").all());
    System.out.println(s.execute("SELECT peer, host_id FROM system.peers").all());
    Collection<Host> hosts = s.getState().getConnectedHosts();
    for (Host host : hosts) {
      System.out.println(host + " " + host.getBroadcastAddress());
    }
    ((SessionManager) s).cluster.manager.controlConnection.triggerReconnect();

    s.close();
    c.close();
  }
}
