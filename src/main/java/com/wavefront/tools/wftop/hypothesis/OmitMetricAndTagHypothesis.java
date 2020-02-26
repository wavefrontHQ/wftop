package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;

import java.util.Objects;

public class OmitMetricAndTagHypothesis extends AbstractUsageDataFPPAdjustingHypothesisImpl {

  private final String metricName;
  private final String tagK;
  private final String tagV;

  public OmitMetricAndTagHypothesis(String metricName, String tagK, String tagV) {
    this.metricName = metricName;
    this.tagK = tagK;
    this.tagV = tagV;
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitMetricAndTagHypothesis(metricName, tagK, tagV);
  }

  @Override
  public String getDescription() {
    return "Eliminate unused metric: \"" + metricName + "\" with the tag: \"" + tagK + "\"=\"" + tagV + "\"";
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (metric.equals(metricName) &&
        pointTags.containsKey(tagK) &&
        pointTags.get(tagK).contains(tagV)) {
      hits.incrementAndGet();
      instancenousRate.mark();
      rate.mark();
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
    OmitMetricAndTagHypothesis that = (OmitMetricAndTagHypothesis) o;
    return metricName.equals(that.metricName) &&
        tagK.equals(that.tagK) &&
        tagV.equals(that.tagV);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, tagK, tagV);
  }
}
