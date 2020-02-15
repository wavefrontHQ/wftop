package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;

import java.util.Objects;

public class OmitMetricPrefixAndTagHypothesis extends AbstractHypothesisImpl {

  private final String prefix;
  private final String tagK;
  private final String tagV;

  public OmitMetricPrefixAndTagHypothesis(String prefix, String tagK, String tagV) {
    this.prefix = prefix;
    this.tagK = tagK;
    this.tagV = tagV;
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitMetricPrefixAndTagHypothesis(prefix, tagK, tagV);
  }

  @Override
  public String getDescription() {
    return "Eliminate unused metrics starting with: \"" + prefix + "\" with the tag: \"" + tagK + "\"=\"" + tagV + "\"";
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
