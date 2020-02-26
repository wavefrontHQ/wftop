package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;

import java.util.Objects;

public class OmitExactMetricHypothesis extends AbstractUsageDataFPPAdjustingHypothesisImpl {

  private final String metricName;

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitExactMetricHypothesis(metricName);
  }

  public OmitExactMetricHypothesis(String metricName) {
    this.metricName = metricName;
  }

  @Override
  public String getDescription() {
    return "Eliminate the unused metric: \"" + metricName + "\"";
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
