package com.wavefront.tools.wftop.components;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.UniformReservoir;
import com.google.common.util.concurrent.AtomicDouble;
import net.agkn.hll.HLL;

import java.util.Map;

/**
 * Interface for nodes, implemented by namespace and source nodes.
 *
 * @author Joanna Ko (kjoanna@vmware.com)
 */
public interface Node<T extends Node> {

  /**
   * @return Name of metric.
   */
  String getValue();

  /**
   * A namespace with only one child node is combined to create a full namespace string.
   *
   * @return Full namespace string of parent and child node.
   */
  String getFlattened();

  /**
   * @return Estimated host cardinality.
   */
  long getEstimatedHostCardinality();

  /**
   * @return Estimated metric cardinality.
   */
  long getEstimatedMetricCardinality();

  /**
   * Lag is calculated difference between wall-clock of running machine compared to ingested timestamp.
   *
   * @return Lag histogram.
   */
  Histogram getLag();

  /**
   * Points found to be accessed in last X days are added to Accessed count.
   *
   * @return returns number of Accessed count.
   */
  int getAccessed();

  /**
   * Default sampling rate at 1%.
   *
   * @return Sampling rate.
   */
  Meter getRate();

  /**
   * Branch limit of node is the maximum children per node.
   * Default is 1,000 nodes.
   *
   * @return if node has reached its branch limit.
   */
  boolean isLimited();

  /**
   * @return Minimum value of Node.
   */
  AtomicDouble getMin();

  /**
   * @return Maximum value of Node.
   */
  AtomicDouble getMax();

  /**
   * @return Range of values metric reports.
   */
  double getRange();

  /**
   * @return Map of current Node's children nodes.
   */
  Map<String, T> getNodes();
}
