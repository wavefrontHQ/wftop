package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Objects;

public class OmitMetricPrefixHypothesis extends AbstractUsageDataFPPAdjustingHypothesisImpl {

  private final String prefix;

  public OmitMetricPrefixHypothesis(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitMetricPrefixHypothesis(prefix);
  }

  @Override
  public String getDescription() {
    return "Eliminate unused metrics with the prefix \"" + prefix + "\"";
  }

  @Override
  public List<String> getDimensions() {
    return Lists.newArrayList(prefix);
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
