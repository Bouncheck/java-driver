package com.datastax.driver.core;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

/*@CCMConfig(startSniProxy = true, numberOfNodes = 1, config = {"native_transport_port:9042"})*/
public class WorkInProgressNameDontGitPushIntegrationTest {

    CCMBridge.Builder ccmbuilder;
    CCMBridge bridge;
    Cluster cluster;

    @Test
    public void tryParsing() throws InterruptedException, IOException {
        ccmbuilder = CCMBridge.builder().withNodes(1).withSniProxy().withBinaryPort(9042);
        bridge = ccmbuilder.build();
        File ccmdir = bridge.getCcmDir();
        File clusterFile = new File(ccmdir, bridge.getClusterName());
        File yamlFile = new File(clusterFile, "config_data.yaml");
        ConnectionConfig config = ConnectionConfig.fromFile(yamlFile);
        System.out.println(config.toString());


        Thread.sleep(1000 * 60 * 2);
    }
}
