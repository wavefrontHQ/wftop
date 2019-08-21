package com.wavefront.tools.wftop.components;

import com.google.common.annotations.VisibleForTesting;

import java.util.Map;

/**
 * Ingest metric names and produce a tree of namespaces.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class NamespaceBuilder {

  private String separators = ".-_";
  private int depthLimit = 10;
  private int branchLimit = 1000;

  private NamespaceNode root = new NamespaceNode("");

  public void setSeparatorCharacters(String separators) {
    this.separators = separators;
    this.root = new NamespaceNode("");
  }

  public NamespaceNode getRoot() {
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
    NamespaceNode curr = root;
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
        Map<String, NamespaceNode> lookupTable = curr.nodes;
        if (lookupTable.size() == branchLimit || curr.limited) {
          curr.limited = true;
          bail = true;
          break;
        }
        NamespaceNode node = lookupTable.computeIfAbsent(soFar, NamespaceNode::new);
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
      Map<String, NamespaceNode> lookupTable = curr.nodes;
      NamespaceNode node = lookupTable.computeIfAbsent(soFar, NamespaceNode::new);
      node.rate.mark();
      if (accessed) node.accessed++;
      node.lag.update(lag);
      updateNodeMinMax(value, node);
    }
  }
  @VisibleForTesting
  public void reset() {
    this.root = new NamespaceNode("");
  }

  @VisibleForTesting
  public int getMaxDepth() {
    return depthLimit;
  }

  @VisibleForTesting
  public int getMaxChildren() {
    return branchLimit;
  }

  private void updateNodeMinMax(double value, NamespaceNode node) {
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

  public void setMaxDepth(int maxDepth) {
    this.depthLimit = maxDepth;
  }

  public void setMaxChildren(int maxChildren) {
    this.branchLimit = maxChildren;
  }

  private void updateCardinality(NamespaceNode node, String metric, String host, MurmurHash3.LongPair reuse) {
    byte[] hostBytes = host.getBytes();
    MurmurHash3.murmurhash3_x64_128(hostBytes, 0, hostBytes.length, 0, reuse);
    node.hostCardinality.addRaw(reuse.val1);
    byte[] metricBytes = metric.getBytes();
    MurmurHash3.murmurhash3_x64_128(metricBytes, 0, metricBytes.length, 0, reuse);
    node.metricCardinality.addRaw(reuse.val1);
  }
}
