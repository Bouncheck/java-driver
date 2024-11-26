package com.datastax.driver.core;

public class TabletMapListener extends SchemaChangeListenerBase {
  private final TabletMap tabletMap;

  public TabletMapListener(TabletMap tabletMap) {
    this.tabletMap = tabletMap;
  }

  @Override
  public void onTableChanged(TableMetadata current, TableMetadata previous) {
    tabletMap.removeTableMappings(previous.getKeyspace().getName(), previous.getName());
  }

  @Override
  public void onTableRemoved(TableMetadata table) {
    tabletMap.removeTableMappings(table.getKeyspace().getName(), table.getName());
  }

  @Override
  public void onKeyspaceRemoved(KeyspaceMetadata keyspace) {
    tabletMap.removeTableMappings(keyspace.getName());
  }

  @Override
  public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous) {
    tabletMap.removeTableMappings(previous.getName());
  }
}
