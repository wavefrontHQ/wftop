package com.wavefront.tools.wftop.hypothesis;

/**
 * For all {@link Hypothesis} that needs adjustment from usage data FPP rate.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class AbstractUsageDataFPPAdjustingHypothesisImpl extends AbstractHypothesisImpl {

  @Override
  public double getViolationPercentage(long usageLookupDays, double usageFPPRate) {
    double expectedAccuracy = Math.pow(1 - usageFPPRate, usageLookupDays);
    double originalViolationPercentage = super.getViolationPercentage(usageLookupDays, usageFPPRate);
    double confidence = 1.0 - originalViolationPercentage;
    if (confidence >= expectedAccuracy) {
      return 0;
    } else {
      return 1.0 - (confidence / expectedAccuracy);
    }
  }
}
