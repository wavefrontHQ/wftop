package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.MurmurHash3;
import net.agkn.hll.HLL;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MetricIsConstantHypothesis extends AbstractHypothesisImpl {

  private final String metricName;
  private final Map<Multimap<String, String>, Double> values = new ConcurrentHashMap<>();
  private final HLL cardinality = new HLL(13, 5);
  private final int maxTrackedSeries;
  private final MurmurHash3.LongPair longPair = new MurmurHash3.LongPair();

  public MetricIsConstantHypothesis(String metricName, int maxTrackedSeries) {
    this.metricName = metricName;
    this.maxTrackedSeries = maxTrackedSeries;
  }

  @Override
  public double getViolationPercentage() {
    return super.getViolationPercentage() * values.size() / getEstimatedTotalSeriesCount();
  }

  @Override
  public String getDescription() {
    return "Eliminate the metric: \"" + metricName + "\" which is always reporting a constant. Tracking: " +
        values.size() + " out of: " + getEstimatedTotalSeriesCount() +
        " series (estimated), observed constant values (limited to 10): [" +
        (values.values().stream().
            sorted().
            distinct().
            limit(10).
            map(v -> MessageFormat.format("{0,number,#.##}", v)).
            collect(Collectors.joining(", ")) + "]");
  }

  private long getEstimatedTotalSeriesCount() {
    return values.size() < maxTrackedSeries ? values.size() : Math.max(cardinality.cardinality(), maxTrackedSeries);
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new MetricIsConstantHypothesis(metricName, maxTrackedSeries);
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host, Multimap<String, String> pointTags,
                                    long timestamp, double value) {
    if (metric.equals(metricName)) {
      rate.mark();
      instancenousRate.mark();

      Multimap<String, String> cloned = ImmutableMultimap.<String, String>builder().
          putAll(pointTags).put("source", host).build();
      longPair.val1 = 0;
      longPair.val2 = 0;
      for (Map.Entry<String, String> entry : cloned.entries()) {
        byte[] b = entry.getKey().getBytes();
        MurmurHash3.murmurhash3_x64_128(b, 0, b.length, (int) longPair.val1, longPair);
        b = entry.getValue().getBytes();
        MurmurHash3.murmurhash3_x64_128(b, 0, b.length, (int) longPair.val1, longPair);
      }
      cardinality.addRaw(longPair.val1);
      Double expected = values.get(cloned);
      if (expected == null && values.size() < maxTrackedSeries) {
        hits.incrementAndGet();
        values.put(cloned, value);
      } else if (expected != null) {
        hits.incrementAndGet();
        if (!expected.equals(value)) {
          violations.incrementAndGet();
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetricIsConstantHypothesis that = (MetricIsConstantHypothesis) o;
    return Objects.equals(metricName, that.metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName);
  }
}
