package com.wavefront.tools.wftop.components;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.UniformReservoir;
import com.google.common.collect.Iterables;

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

  public synchronized void accept(String input, long timestamp, boolean accessed) {
    root.rate.mark();
    if (accessed) root.accessed++;
    long lag = System.currentTimeMillis() - timestamp;
    root.lag.update(lag);
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
        if (accessed) node.accessed++;
        node.lag.update(lag);
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
    }
  }

  public void reset() {
    this.root = new Node("");
  }

  public class Node {
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final String value;
    private final Histogram lag = new Histogram(new UniformReservoir());
    private final Meter rate = new Meter();
    private int accessed = 0;
    private boolean limited = false;

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
  }
}
