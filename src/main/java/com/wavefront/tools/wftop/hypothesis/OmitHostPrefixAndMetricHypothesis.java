package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Objects;

public class OmitHostPrefixAndMetricHypothesis extends AbstractUsageDataFPPAdjustingHypothesisImpl {

  private final String hostPrefix;
  private final String metric;

  public OmitHostPrefixAndMetricHypothesis(String hostPrefix, String metric) {
    this.hostPrefix = hostPrefix;
    this.metric = metric;
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitHostPrefixAndMetricHypothesis(hostPrefix, metric);
  }

  @Override
  public String getDescription() {
    return "Eliminate unused metric: \"" + metric + "\" for host prefix: \"" + hostPrefix + "\"";
  }

  @Override
  public List<String> getDimensions() {
    return Lists.newArrayList(metric, "source=" + hostPrefix);
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (host.startsWith(hostPrefix) && this.metric.equals(metric)) {
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
    OmitHostPrefixAndMetricHypothesis that = (OmitHostPrefixAndMetricHypothesis) o;
    return hostPrefix.equals(that.hostPrefix) &&
        metric.equals(that.metric);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostPrefix, metric);
  }
}
