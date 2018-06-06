package com.netflix.sps.data;

public class StartEvent {
  private static final String SUCCESS = "success";
  private String device;
  private String sev;
  private String title;
  private String country;
  private long time;

  public String getSev() {
    return sev;
  }

  public void setSev(String sev) {
    this.sev = sev;
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

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public boolean isSuccess() {
    return this.sev.equals(SUCCESS);
  }
}
