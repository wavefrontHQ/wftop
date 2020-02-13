package com.wavefront.tools.wftop.hypothesis;

import com.codahale.metrics.Meter;
import com.google.common.collect.Multimap;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class OmitMetricPrefixHypothesis implements Hypothesis {

  private final String prefix;

  private Meter rate = new Meter();
  private Meter instancenousRate = new Meter();
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong violations = new AtomicLong();

  public OmitMetricPrefixHypothesis(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Hypothesis clone() {
    return new OmitMetricPrefixHypothesis(prefix);
  }

  @Override
  public String getDescription() {
    return "Eliminate all metrics with the prefix \"" + prefix + "\"";
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
    if (metric.startsWith(prefix)) {
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
    OmitMetricPrefixHypothesis that = (OmitMetricPrefixHypothesis) o;
    return prefix.equals(that.prefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix);
  }
}
