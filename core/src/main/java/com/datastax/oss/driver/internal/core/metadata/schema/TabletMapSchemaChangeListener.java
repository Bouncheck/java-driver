package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TabletMapSchemaChangeListener extends SchemaChangeListenerBase {
  private final TabletMap tabletMap;

  public TabletMapSchemaChangeListener(TabletMap tabletMap) {
    this.tabletMap = tabletMap;
  }

  @Override
  public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
    tabletMap.removeByKeyspace(keyspace.getName());
  }

  @Override
  public void onKeyspaceUpdated(
      @NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
    tabletMap.removeByKeyspace(previous.getName());
  }

  @Override
  public void onTableDropped(@NonNull TableMetadata table) {
    tabletMap.removeByTable(table.getName());
  }

  @Override
  public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
    tabletMap.removeByTable(previous.getName());
  }
}
