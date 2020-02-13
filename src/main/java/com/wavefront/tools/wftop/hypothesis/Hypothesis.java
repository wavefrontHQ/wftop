package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;
import wavefront.report.ReportPoint;

/**
 * A hypothesis to reduce usage.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public interface Hypothesis {
  /**
   * @return Human understandable description of the hypothesis
   */
  String getDescription();

  Hypothesis clone();

  /**
   * @return The PPS savings of this hypothesis if enacted.
   */
  double getRawPPSSavings();

  default double getPPSSavings(int numBackends, double sampleRate) {
    return getRawPPSSavings() * numBackends / sampleRate;
  }

  double getInstaneousRate();

  /**
   * @return How often is this hypothesis violated (between 0 and 1).
   */
  double getViolationPercentage();

  /**
   * Process a {@link ReportPoint} and update stats.
   *
   * @return Whether the point was culled by this hypothesis.
   */
  boolean processReportPoint(boolean accessed, String metric, String host,
                             Multimap<String, String> pointTags, long timestamp, double value);

  void reset();
}
