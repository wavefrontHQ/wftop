package com.wavefront.tools.wftop.hypothesis;

import com.codahale.metrics.Meter;
import com.google.common.collect.Multimap;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class OmitMetricPrefixAndTagHypothesis implements Hypothesis {

  private final String prefix;
  private final String tagK;
  private final String tagV;

  private Meter rate = new Meter();
  private Meter instancenousRate = new Meter();
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong violations = new AtomicLong();

  public OmitMetricPrefixAndTagHypothesis(String prefix, String tagK, String tagV) {
    this.prefix = prefix;
    this.tagK = tagK;
    this.tagV = tagV;
  }

  @Override
  public Hypothesis clone() {
    return new OmitMetricPrefixAndTagHypothesis(prefix, tagK, tagV);
  }

  @Override
  public String getDescription() {
    return "Eliminate metrics starting with: \"" + prefix + "\" with the tag: \"" + tagK + "\"=\"" + tagV + "\"";
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
    if (metric.startsWith(prefix) && pointTags.containsKey(tagK) && pointTags.get(tagK).contains(tagV)) {
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
    OmitMetricPrefixAndTagHypothesis that = (OmitMetricPrefixAndTagHypothesis) o;
    return prefix.equals(that.prefix) &&
        tagK.equals(that.tagK) &&
        tagV.equals(that.tagV);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, tagK, tagV);
  }
}
