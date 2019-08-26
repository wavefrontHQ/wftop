package com.wavefront.tools.wftop.components;

import com.google.common.collect.Multimap;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.AtomicDouble;

import java.util.Map;

/**
 * Groups metrics based on source of ingestion. Default source is set to "None".
 *
 * @author Joanna Ko (kjoanna@vmware.com).
 */
public class SourceNode implements Node<NamespaceNode> {

  private final String value;
  private final NamespaceBuilder namespaceBuilder = new NamespaceBuilder();

  public SourceNode(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String getFlattened() {
    return getValue();
  }

  @Override
  public long getEstimatedHostCardinality() {
    return namespaceBuilder.getRoot().getEstimatedHostCardinality();
  }

  @Override
  public long getEstimatedMetricCardinality() {
    return namespaceBuilder.getRoot().getEstimatedMetricCardinality();
  }

  @Override
  public Histogram getLag() {
    return namespaceBuilder.getRoot().getLag();
  }

  @Override
  public int getAccessed() {
    return namespaceBuilder.getRoot().getAccessed();
  }

  @Override
  public Meter getRate() {
    return namespaceBuilder.getRoot().getRate();
  }

  @Override
  public boolean isLimited() {
    return namespaceBuilder.getRoot().isLimited();
  }

  @Override
  public AtomicDouble getMin() {
    return namespaceBuilder.getRoot().getMin();
  }

  @Override
  public AtomicDouble getMax() {
    return namespaceBuilder.getRoot().getMax();
  }

  @Override
  public double getRange() {
    return getMax().get() - getMin().get();
  }

  @Override
  public Map<String, NamespaceNode> getNodes() {
    return namespaceBuilder.getRoot().getNodes();
  }

  /**
   * Adds ID to NamespaceBuilder.
   *
   * @param name Name of ID.
   */
  public void accept(String name) {
    namespaceBuilder.accept(name, name, name, 0, 0,
        false, false);
  }

  /**
   * Adds point to NamespaceBuilder.
   *
   * @param analysisDimension Analysis Dimension chosen.
   * @param accessed          % Accessed.
   * @param metric            Metric name.
   * @param host              Host name.
   * @param pointTags         Point Tags map.
   * @param timestamp         Time of metric ingestion.
   * @param value             Point value.
   */
  public void accept(Dimension analysisDimension, boolean accessed, String metric, String host,
                     Multimap<String, String> pointTags, long timestamp, double value) {
    if (analysisDimension == Dimension.METRIC) {
      namespaceBuilder.accept(metric, host, metric, timestamp, value, accessed, true);
    } else if (analysisDimension == Dimension.HOST) {
      namespaceBuilder.accept(host, host, metric, timestamp, value, accessed, true);
    } else if (analysisDimension == Dimension.POINT_TAG) {
      // here we are over-counting.
      for (Map.Entry<String, String> entry : pointTags.entries()) {
        namespaceBuilder.accept(entry.getKey() + "=" + entry.getValue(), host, metric,
            timestamp, value, accessed, true);
      }
    } else if (analysisDimension == Dimension.POINT_TAG_KEY) {
      // here we are over-counting.
      for (String entry : pointTags.keySet()) {
        namespaceBuilder.accept(entry, host, metric, timestamp, value, accessed, true);
      }
    }
  }

  /**
   * @return NamespaceBuilder of SourceNode.
   */
  public NamespaceBuilder getNamespaceBuilder() {
    return namespaceBuilder;
  }

  public void setSeparatorCharacters(String separators) {
    namespaceBuilder.setSeparatorCharacters(separators);
  }

  public void setMaxDepth(int maxDepth) {
    namespaceBuilder.setMaxDepth(maxDepth);
  }

  public void setMaxChildren(int maxChildren) {
    namespaceBuilder.setMaxChildren(maxChildren);
  }
}
