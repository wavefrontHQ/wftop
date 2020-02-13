package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Multimap;

import java.util.ArrayList;
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

  public List<Hypothesis> removeAllViolatingHypothesis(double upperBound) {
    synchronized (this) {
      for (Hypothesis h : hypothesisList) {
        if (h.getViolationPercentage() > confidence) {
          blacklistedHypothesis.add(h);
        }
      }
      List<Hypothesis> toReturn = new ArrayList<>();
      for (Hypothesis h : hypothesisList) {
        if (h.getViolationPercentage() > confidence ||
            h.getViolationPercentage() <= upperBound) {
          toReturn.add(h);
        }
      }
      hypothesisList.removeAll(toReturn);
      sortHypothesisByInstantaneousRate();
      return toReturn;
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

  public void trimLowPPSHypothesis(int maxRecommendations) {
    double average = hypothesisList.stream().
        filter(h -> h.getAge() > 1).
        mapToDouble(Hypothesis::getInstaneousRate).summaryStatistics().
        getAverage();
    long count = hypothesisList.stream().filter(h -> h.getAge() > 1 && h.getInstaneousRate() < average).count();
    if (count < maxRecommendations) return;
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
}

