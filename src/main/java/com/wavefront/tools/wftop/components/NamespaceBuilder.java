package com.wavefront.tools.wftop.components;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.UniformReservoir;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AtomicDouble;
import net.agkn.hll.HLL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ingest metric names and produce a tree of namespaces.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class NamespaceBuilder {

  private String separators = ".-_";
  private int depthLimit = 10;
  private int branchLimit = 1000;

  private Node root = new Node("");

  public void setSeparatorCharacters(String separators) {
    this.separators = separators;
    this.root = new Node("");
  }

  public String getSeparatorCharacters() {
    return separators;
  }

  public Node getRoot() {
    return root;
  }

  public synchronized void accept(String input, String host, String metric, long timestamp, double value,
                                  boolean accessed) {
    root.rate.mark();
    MurmurHash3.LongPair longPair = new MurmurHash3.LongPair();
    updateCardinality(root, metric, host, longPair);
    if (accessed) root.accessed++;
    long lag = System.currentTimeMillis() - timestamp;
    root.lag.update(lag);
    updateNodeMinMax(value, root);
    Node curr = root;
    StringBuilder sb = new StringBuilder();
    int depth = 0;
    boolean bail = false;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      boolean separator = false;
      for (int j = 0; j < separators.length(); j++) {
        if (c == separators.charAt(j)) {
          separator = true;
          break;
        }
      }
      sb.append(c);
      if (separator) {
        String soFar = sb.toString();
        Map<String, Node> lookupTable = curr.nodes;
        if (lookupTable.size() == branchLimit || curr.limited) {
          curr.limited = true;
          bail = true;
          break;
        }
        Node node = lookupTable.computeIfAbsent(soFar, Node::new);
        node.rate.mark();
        updateCardinality(node, metric, host, longPair);
        if (accessed) node.accessed++;
        node.lag.update(lag);
        updateNodeMinMax(value, node);
        curr = node;
        sb.setLength(0);
        depth++;
        if (depth == depthLimit) {
          bail = true;
          break;
        }
      }
    }
    // might have another node at the end.
    if (!bail && sb.length() > 0) {
      String soFar = sb.toString();
      Map<String, Node> lookupTable = curr.nodes;
      Node node = lookupTable.computeIfAbsent(soFar, Node::new);
      node.rate.mark();
      if (accessed) node.accessed++;
      node.lag.update(lag);
      updateNodeMinMax(value, node);
    }
  }

  private void updateNodeMinMax(double value, Node node) {
    while (true) {
      double min = node.min.get();
      if (min <= value) break;
      if (node.min.compareAndSet(min, value)) break;
    }
    while (true) {
      double max = node.max.get();
      if (max >= value) break;
      if (node.max.compareAndSet(max, value)) break;
    }
  }

  public void reset() {
    this.root = new Node("");
  }

  public int getMaxDepth() {
    return depthLimit;
  }

  public int getMaxChildren() {
    return branchLimit;
  }

  public void setMaxDepth(int maxDepth) {
    this.depthLimit = maxDepth;
  }

  public void setMaxChildren(int maxChildren) {
    this.branchLimit = maxChildren;
  }

  private void updateCardinality(Node node, String metric, String host, MurmurHash3.LongPair reuse) {
    byte[] hostBytes = host.getBytes();
    MurmurHash3.murmurhash3_x64_128(hostBytes, 0, hostBytes.length, 0, reuse);
    node.hostCardinality.addRaw(reuse.val1);
    byte[] metricBytes = metric.getBytes();
    MurmurHash3.murmurhash3_x64_128(metricBytes, 0, metricBytes.length, 0, reuse);
    node.metricCardinality.addRaw(reuse.val1);
  }

  public class Node {
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final String value;
    private final Histogram lag = new Histogram(new UniformReservoir());
    private final Meter rate = new Meter();
    private final HLL hostCardinality = new HLL(13, 5);
    private final HLL metricCardinality = new HLL(13, 5);
    private int accessed = 0;
    private boolean limited = false;
    private final AtomicDouble min = new AtomicDouble(Double.MAX_VALUE);
    private final AtomicDouble max = new AtomicDouble(-Double.MAX_VALUE);

    public Node(String value) {
      this.value = value;
    }

    public Map<String, Node> getNodes() {
      return nodes;
    }

    public String getValue() {
      return value;
    }

    public String getFlattened() {
      if (nodes.size() == 1) {
        return value + Iterables.getOnlyElement(nodes.values()).getFlattened();
      }
      return value;
    }

    public long getEstimatedHostCardinality() {
      return hostCardinality.cardinality();
    }

    public long getEstimatedMetricCardinality() {
      return metricCardinality.cardinality();
    }

    public Histogram getLag() {
      return lag;
    }

    public int getAccessed() {
      return accessed;
    }

    public Meter getRate() {
      return rate;
    }

    public boolean isLimited() {
      return limited;
    }

    public AtomicDouble getMin() {
      return min;
    }

    public AtomicDouble getMax() {
      return max;
    }

    public double getRange() {
      return max.get() - min.get();
    }
  }
}
