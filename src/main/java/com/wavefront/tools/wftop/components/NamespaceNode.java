package com.wavefront.tools.wftop.components;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AtomicDouble;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.UniformReservoir;
import net.agkn.hll.HLL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceNode implements Node<NamespaceNode> {

  final Map<String, NamespaceNode> nodes = new ConcurrentHashMap<>();
  private final String value;
  final Histogram lag = new Histogram(new UniformReservoir());
  final Meter rate = new Meter();
  final HLL hostCardinality = new HLL(13, 5);
  final HLL metricCardinality = new HLL(13, 5);
  int accessed = 0;
  boolean limited = false;

  final AtomicDouble min = new AtomicDouble(Double.MAX_VALUE);
  final AtomicDouble max = new AtomicDouble(-Double.MAX_VALUE);

  NamespaceNode(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String getFlattened() {
    if (nodes.size() == 1) {
      return value + Iterables.getOnlyElement(nodes.values()).getFlattened();
    }
    return value;
  }

  @Override
  public long getEstimatedHostCardinality() {
    return hostCardinality.cardinality();
  }

  @Override
  public long getEstimatedMetricCardinality() {
    return metricCardinality.cardinality();
  }

  @Override
  public Histogram getLag() {
    return lag;
  }

  @Override
  public int getAccessed() {
    return accessed;
  }

  @Override
  public Meter getRate() {
    return rate;
  }

  @Override
  public boolean isLimited() {
    return limited;
  }

  @Override
  public AtomicDouble getMin() {
    return min;
  }

  @Override
  public AtomicDouble getMax() {
    return max;
  }

  @Override
  public double getRange() {
    return max.get() - min.get();
  }

  @Override
  public Map<String, NamespaceNode> getNodes() {
    return nodes;
  }
}
