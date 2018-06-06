package com.netflix.sps.data;

public class StartEventResult {
  private String device;
  private String title;
  private String country;
  private long sps;

  public StartEventResult(StartEvent startEvent) {
    this.device = startEvent.getDevice();
    this.title = startEvent.getTitle();
    this.country = startEvent.getCountry();
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public long getSps() {
    return sps;
  }

  public void setSps(long sps) {
    this.sps = sps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StartEventResult that = (StartEventResult) o;

    if (sps != that.sps) {
      return false;
    }
    if (!device.equals(that.device)) {
      return false;
    }
    if (!title.equals(that.title)) {
      return false;
    }
    return country.equals(that.country);
  }

  @Override
  public int hashCode() {
    int result = device.hashCode();
    result = 31 * result + title.hashCode();
    result = 31 * result + country.hashCode();
    result = 31 * result + (int) (sps ^ (sps >>> 32));
    return result;
  }
}
