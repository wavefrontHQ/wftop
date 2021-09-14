package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class MetricIsAlwaysOldHypothesis extends AbstractHypothesisImpl {

  private final String metricName;

  @Override
  public Hypothesis cloneHypothesis() {
    return new MetricIsAlwaysOldHypothesis(metricName);
  }

  public MetricIsAlwaysOldHypothesis(String metricName) {
    this.metricName = metricName;
  }

  @Override
  public String getDescription() {
    return "Eliminate the metric: \"" + metricName + "\" which is always reporting more than an hour ago in the past";
  }

  @Override
  public List<String> getDimensions() {
    return Lists.newArrayList(metricName);
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (metric.equals(metricName)) {
      hits.incrementAndGet();
      rate.mark();
      instancenousRate.mark();
      if (timestamp > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) {
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
    MetricIsAlwaysOldHypothesis that = (MetricIsAlwaysOldHypothesis) o;
    return metricName.equals(that.metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName);
  }
}
