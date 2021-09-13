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

  Hypothesis cloneHypothesis();

  /**
   * @return The PPS savings of this hypothesis if enacted.
   */
  double getRawPPSSavingsRate(boolean lifetime);

  default double getPPSSavings(boolean lifetime, int numBackends, double sampleRate) {
    return getRawPPSSavingsRate(lifetime) * numBackends / sampleRate;
  }

  double getInstaneousRate();

  /**
   * @param usageLookupDays The number of days we are looking back usage data.
   * @param usageFPPRate    The false positive rate of lookback data.
   * @return How often is this hypothesis violated (between 0 and 1).
   */
  double getViolationPercentage(long usageLookupDays, double usageFPPRate);

  /**
   * Process a {@link ReportPoint} and update stats.
   *
   * @return Whether the point was culled by this hypothesis.
   */
  boolean processReportPoint(boolean accessed, String metric, String host,
                             Multimap<String, String> pointTags, long timestamp, double value);

  void reset();

  int getAge();

  void incrementAge();

  void resetAge();
}
