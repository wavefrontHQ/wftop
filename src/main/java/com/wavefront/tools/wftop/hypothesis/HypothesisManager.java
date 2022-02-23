package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.hypothesis.pojo.HypothesisEvalResult;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manage a list of hypothesis within {@link #maxHypothesisCount}
 * Sort base on {@link Hypothesis#getInstaneousRate()}
 * Drop base on {@link Hypothesis#getViolationPercentage()} ()}
 */
public class HypothesisManager {

  private final int maxHypothesisCount;
  private final double confidence;
  private final long usageLookbackDays;
  private final double usageFPPRate;
  private final AtomicInteger droppedHypothesis = new AtomicInteger();
  private final List<Hypothesis> hypothesisList = new CopyOnWriteArrayList<>();
  private final Set<Hypothesis> blacklistedHypothesis = new HashSet<>();

  private final AtomicBoolean fullEvaluationMode = new AtomicBoolean(true);

  public HypothesisManager(int maxHypothesisCount, double confidence, long usageLookbackDays, double usageFPPRate) {
    this.maxHypothesisCount = maxHypothesisCount;
    this.confidence = confidence;
    this.usageLookbackDays = usageLookbackDays;
    this.usageFPPRate = usageFPPRate;
  }

  public boolean isFull() {
    return hypothesisList.size() >= maxHypothesisCount;
  }

  public double getConfidence() {
    return confidence;
  }

  public boolean offerHypothesis(Hypothesis hypothesis) {
    if (blacklistedHypothesis.contains(hypothesis)) return false;
    if (hypothesisList.size() > maxHypothesisCount) {
      droppedHypothesis.incrementAndGet();
      return false;
    }
    hypothesisList.add(hypothesis.cloneHypothesis());
    return true;
  }

  public void incrementAge() {
    for (Hypothesis h : hypothesisList) {
      h.incrementAge();
    }
  }

  public void resetDroppedCounter() {
    droppedHypothesis.set(0);
  }

  public int getDroppedCount() {
    return droppedHypothesis.get();
  }

  public void evaluateAllHypothesis() {
    fullEvaluationMode.set(true);
  }

  public void enableRankedHypothesisEvaluation() {
    fullEvaluationMode.set(false);
  }

  /**
   * Evaluate and move the maintained hypothesisList accordingly.
   *
   * @param upperBound accepted upperBound
   * @return hypothesis that violates the confidences, or still within upperBound.
   */
  public HypothesisEvalResult removeAllViolatingHypothesis(double upperBound) {
    synchronized (this) {
      HypothesisEvalResult res = HypothesisEvalResult.newHypothesisEvalResult();
      for (Hypothesis h : hypothesisList) {
        if (h.getViolationPercentage() <= upperBound) {
          res.getWithinBounds().add(h);
        }

        if (h.getViolationPercentage() > confidence) {
          res.getRejected().add(h);
          blacklistedHypothesis.add(h);
        }
      }

      hypothesisList.removeAll(res.getRejected());
      hypothesisList.removeAll(res.getWithinBounds());
      sortHypothesisByInstantaneousRate();
      return res;
    }
  }

  public void sortHypothesisByInstantaneousRate() {
    synchronized (this) {
      while (true) {
        try {
          hypothesisList.sort((o1, o2) -> Double.compare(o2.getInstaneousRate(), o1.getInstaneousRate()));
          return;
        } catch (IllegalArgumentException ex) {
          // just resort.
        }
      }
    }
  }

  /**
   * Trim by average and only if enough recommendations are provided.
   */
  public void trimLowPPSHypothesis(int maxRecommendations) {
    double average = hypothesisList.stream().
        filter(h -> h.getAge() > 1).
        mapToDouble(Hypothesis::getInstaneousRate).summaryStatistics().
        getAverage();
    long toRemove = hypothesisList.stream().filter(h -> h.getAge() > 1 && h.getInstaneousRate() < average).count();
    if (hypothesisList.size() - toRemove < maxRecommendations) return;
    hypothesisList.removeIf(h -> h.getAge() > 1 && h.getInstaneousRate() < average);
  }

  public boolean consumeReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value, int depth) {
    if (fullEvaluationMode.get()) {
      boolean admitted = false;
      int curr = 1;
      for (Hypothesis hypothesis : hypothesisList) {
        if (curr > depth) {
          hypothesis.processReportPoint(accessed, metric, host, pointTags, timestamp, value);
        } else {
          admitted |= hypothesis.processReportPoint(accessed, metric, host, pointTags, timestamp, value);
        }
        curr++;
      }
      return admitted;
    } else {
      int curr = 1;
      for (Hypothesis hypothesis : hypothesisList) {
        if (hypothesis.processReportPoint(accessed, metric, host, pointTags, timestamp, value)) {
          return curr <= depth;
        }
        curr++;
      }
      return false;
    }
  }

  public List<Hypothesis> getAllHypothesis() {
    return hypothesisList;
  }

  public void resetInstantaneousRateForAllHypothesis() {
    hypothesisList.forEach(Hypothesis::reset);
  }

  public void addHypothesis(Hypothesis h) {
    h.resetAge();
    blacklistedHypothesis.remove(h);
    hypothesisList.add(h);
  }

  public int getBlacklistedHypothesis() {
    return blacklistedHypothesis.size();
  }
}

