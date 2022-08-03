package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Context {
  private final String datacenterName;
  private final String authInfoName;

  public Context(
      @JsonProperty(value = "datacenterName", required = true) String datacenterName,
      @JsonProperty(value = "authInfoName", required = true) String authInfoName) {
    this.datacenterName = datacenterName;
    this.authInfoName = authInfoName;
  }
}
