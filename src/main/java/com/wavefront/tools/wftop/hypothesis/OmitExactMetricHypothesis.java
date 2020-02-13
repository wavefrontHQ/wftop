package com.wavefront.tools.wftop.hypothesis;

import com.codahale.metrics.Meter;
import com.google.common.collect.Multimap;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class OmitExactMetricHypothesis implements Hypothesis {

  private final String metricName;

  private Meter rate = new Meter();
  private Meter instancenousRate = new Meter();
  private final AtomicLong hits = new AtomicLong(1);
  private final AtomicLong violations = new AtomicLong();

  @Override
  public Hypothesis clone() {
    return new OmitExactMetricHypothesis(metricName);
  }

  public OmitExactMetricHypothesis(String metricName) {
    this.metricName = metricName;
  }

  @Override
  public String getDescription() {
    return "Eliminate the metric: \"" + metricName + "\"";
  }

  @Override
  public double getRawPPSSavings() {
    return rate.getFifteenMinuteRate();
  }

  @Override
  public double getInstaneousRate() {
    return instancenousRate.getMeanRate();
  }

  @Override
  public double getViolationPercentage() {
    return (double) violations.get() / hits.get();
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (metric.equals(metricName)) {
      hits.incrementAndGet();
      rate.mark();
      instancenousRate.mark();
      if (accessed) {
        violations.incrementAndGet();
      }
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    instancenousRate = new Meter();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OmitExactMetricHypothesis that = (OmitExactMetricHypothesis) o;
    return metricName.equals(that.metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName);
  }
}
