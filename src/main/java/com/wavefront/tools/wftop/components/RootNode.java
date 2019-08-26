package com.wavefront.tools.wftop.components;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AtomicDouble;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Root of nodes tree, accepts data points and distributes accordingly.
 *
 * @author Joanna Ko (kjoanna@vmware.com)
 */
public class RootNode implements Node<SourceNode> {
  private final String value;
  private final String WAVEFRONT_SOURCE_TAG_KEY = "_wavefront_source";
  private final Map<String, SourceNode> ingestionSource = new ConcurrentHashMap<>();
  private SourceNode globalNode = new SourceNode("None");
  private String separators = ".-_=";
  private int depthLimit = 10;
  private int branchLimit = 1000;

  public RootNode(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String getFlattened() {
    return getValue();
  } //we will never display root; do not need to flatten

  @Override
  public long getEstimatedHostCardinality() {
    return globalNode.getNamespaceBuilder().getRoot().getEstimatedHostCardinality();
  }

  @Override
  public long getEstimatedMetricCardinality() {
    return globalNode.getNamespaceBuilder().getRoot().getEstimatedMetricCardinality();
  }

  @Override
  public Histogram getLag() {
    return globalNode.getNamespaceBuilder().getRoot().getLag();
  }

  @Override
  public int getAccessed() {
    return globalNode.getNamespaceBuilder().getRoot().getAccessed();
  }

  @Override
  public Meter getRate() {
    return globalNode.getNamespaceBuilder().getRoot().getRate();
  }

  @Override
  public boolean isLimited() {
    return globalNode.getNamespaceBuilder().getRoot().isLimited();
  }

  @Override
  public AtomicDouble getMin() {
    return globalNode.getNamespaceBuilder().getRoot().getMin();
  }

  @Override
  public AtomicDouble getMax() {
    return globalNode.getNamespaceBuilder().getRoot().getMax();
  }

  @Override
  public double getRange() {
    return getMax().get() - getMin().get();
  }

  @Override
  public Map<String, SourceNode> getNodes() {
    return ingestionSource;
  }

  /**
   * Accept ID.
   *
   * @param name
   */
  public void accept(String name) {
    ingestionSource.get("None").accept(name);
    globalNode.accept(name);
  }

  /**
   * Accept point.
   *
   * @param analysisDimension      Analysis Dimension chosen.
   * @param groupByIngestionSource Choose whether to group by Ingestion Source.
   * @param accessed               % Accessed.
   * @param metric                 Metric name.
   * @param host                   Host name.
   * @param pointTags              Point Tags map.
   * @param timestamp              Time of metric ingestion.
   * @param value                  Point value.
   */
  public void accept(Dimension analysisDimension, boolean groupByIngestionSource,
                     boolean accessed, String metric, String host,
                     Multimap<String, String> pointTags, long timestamp, double value) {
    String source_value = "None";
    if (groupByIngestionSource) {
      for (Map.Entry<String, String> s : pointTags.entries()) {
        if (s.getKey().equals(WAVEFRONT_SOURCE_TAG_KEY)) {
          source_value = pointTags.get(WAVEFRONT_SOURCE_TAG_KEY).iterator().next();
          break;
        }
      }
      if (ingestionSource.get(source_value) == null) {
        setUpSourceNode(source_value);
      }
    }
    ingestionSource.get(source_value).accept(analysisDimension, accessed, metric, host,
        pointTags, timestamp, value);
    globalNode.accept(analysisDimension, accessed, metric, host,
        pointTags, timestamp, value);
  }

  public String getSeparatorCharacters() {
    return separators;
  }

  /**
   * clears SourceNodes and adds default SourceNode "None".
   */
  public void reset() {
    globalNode = new SourceNode("None");
    ingestionSource.clear();
    ingestionSource.put("None", new SourceNode("None"));
    //set Configurations for NamespaceBuilder
    setSeparatorCharacters(separators);
    setMaxDepth(depthLimit);
    setMaxChildren(branchLimit);
  }

  public int getMaxDepth() {
    return depthLimit;
  }

  public int getMaxChildren() {
    return branchLimit;
  }

  /**
   * @return SourceNode "None", the default when Group By Source not specified.
   */
  public SourceNode getDefaultRoot() {
    for (Map.Entry<String, SourceNode> entry : ingestionSource.entrySet()) {
      if (entry.getKey().equals("None"))
        return entry.getValue();
    }
    return null;
  }

  public void setSeparatorCharacters(String separators) {
    this.separators = separators;
    this.getNodes().forEach((k, v) ->
        v.setSeparatorCharacters(this.separators));
  }

  public void setMaxDepth(int maxDepth) {
    this.depthLimit = maxDepth;
    this.getNodes().forEach((k, v) ->
        v.setMaxDepth(this.depthLimit));
  }

  public void setMaxChildren(int maxChildren) {
    this.branchLimit = maxChildren;
    this.getNodes().forEach((k, v) ->
        v.setMaxChildren(this.branchLimit));
  }

  private void setUpSourceNode(String source_value) {
    ingestionSource.put(source_value, new SourceNode(source_value));
    ingestionSource.get(source_value).setSeparatorCharacters(this.separators);
    ingestionSource.get(source_value).setMaxDepth(this.depthLimit);
    ingestionSource.get(source_value).setMaxChildren(this.branchLimit);
  }
}
