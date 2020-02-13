package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HypothesisManager {

  private final int maxHypothesisCount;
  private final double confidence;
  private final AtomicInteger droppedHypothesis = new AtomicInteger();
  private final List<Hypothesis> hypothesisList = new CopyOnWriteArrayList<>();
  private final Set<Hypothesis> blacklistedHypothesis = new HashSet<>();

  private final AtomicBoolean fullEvaluationMode = new AtomicBoolean(true);

  public HypothesisManager(int maxHypothesisCount, double confidence) {
    this.maxHypothesisCount = maxHypothesisCount;
    this.confidence = confidence;
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
    hypothesisList.add(hypothesis.clone());
    return true;
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

  public void removeAllViolatingHypothesis() {
    synchronized (this) {
      for (Hypothesis h : hypothesisList) {
        if (h.getViolationPercentage() > confidence) {
          blacklistedHypothesis.add(h);
        }
      }
      hypothesisList.removeIf(next -> next.getViolationPercentage() > confidence);
      sortHypothesisByInstantaneousRate();
    }
  }

  public void sortHypothesisByInstantaneousRate() {
    while (true) {
      try {
        hypothesisList.sort((o1, o2) -> Double.compare(o2.getInstaneousRate(), o1.getInstaneousRate()));
        return;
      } catch (IllegalArgumentException ex) {
        // just resort.
      }
    }
  }

  public void trimLowPPSHypothesis() {
    double average = hypothesisList.stream().mapToDouble(Hypothesis::getInstaneousRate).summaryStatistics().
        getAverage();
    hypothesisList.removeIf(next -> next.getInstaneousRate() < average);
  }

  public boolean consumeReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (fullEvaluationMode.get()) {
      boolean admitted = false;
      for (Hypothesis hypothesis : hypothesisList) {
        admitted |= hypothesis.processReportPoint(accessed, metric, host, pointTags, timestamp, value);
      }
      return admitted;
    } else {
      for (Hypothesis hypothesis : hypothesisList) {
        if (hypothesis.processReportPoint(accessed, metric, host, pointTags, timestamp, value)) {
          return true;
        }
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
}

