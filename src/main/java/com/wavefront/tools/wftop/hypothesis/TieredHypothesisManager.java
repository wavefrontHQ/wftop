package com.wavefront.tools.wftop.hypothesis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TieredHypothesisManager {

  private static final Logger log = Logger.getLogger(TieredHypothesisManager.class.getCanonicalName());

  private final List<HypothesisManager> managers = new ArrayList<>();
  private final PointsSpy spy;
  private final int maxRecommendations;
  private final Cache<Hypothesis, Boolean> hypothesisDedupCache = Caffeine.newBuilder().
      expireAfterWrite(5, TimeUnit.MINUTES).build();

  public TieredHypothesisManager(int maxHypothesis, PointsSpy spy, int maxRecommendations, double... confidences) {
    this.spy = spy;
    this.maxRecommendations = maxRecommendations;
    for (double confidence : confidences) {
      managers.add(new HypothesisManager(maxHypothesis, confidence));
    }
  }

  public void run(AtomicInteger numBackends, double minimumPPS, double rateArg) throws InterruptedException {
    for (HypothesisManager hypothesisManager : managers) {
      hypothesisManager.evaluateAllHypothesis();
    }
    spy.start();
    log.info("Warming up for 10s");
    Thread.sleep(10000);
    while (true) {
      if (!spy.isConnected()) {
        log.info("Exiting due to connectivity failure");
        System.exit(1);
      }
      log.info("Evaluating all hypothesis to collect non-cumulative PPS data");
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.evaluateAllHypothesis();
        hypothesisManager.sortHypothesisByInstantaneousRate();
        hypothesisManager.resetInstantaneousRateForAllHypothesis();
        hypothesisManager.resetDroppedCounter();
      }
      Thread.sleep(60000);
      log.info("Begin ranked hypothesis testing");
      double confidence = -1;
      HypothesisManager previous = null;
      List<Hypothesis> toAdd = new ArrayList<>();
      for (HypothesisManager hypothesisManager : managers) {
        bidirectionalProcessHypothesis(confidence, previous, toAdd, hypothesisManager);
        hypothesisManager.enableRankedHypothesisEvaluation();
        hypothesisManager.sortHypothesisByInstantaneousRate();
        hypothesisManager.resetInstantaneousRateForAllHypothesis();
        hypothesisManager.resetDroppedCounter();
        confidence = hypothesisManager.getConfidence();
        previous = hypothesisManager;
      }
      Thread.sleep(60000);
      log.info("Trimming recommendations");
      confidence = -1;
      previous = null;
      for (HypothesisManager hypothesisManager : managers) {
        bidirectionalProcessHypothesis(confidence, previous, toAdd, hypothesisManager);
        hypothesisManager.trimLowPPSHypothesis();
        confidence = hypothesisManager.getConfidence();
        previous = hypothesisManager;
      }
      for (HypothesisManager hypothesisManager : managers) {
        if (hypothesisManager.getConfidence() >= 1.0) continue; // ignore the catch-all hypothesis.
        List<Hypothesis> results = hypothesisManager.getAllHypothesis();
        List<Hypothesis> candidates = results.stream().
            filter(s -> s.getAge() > 2).
            filter(s -> s.getPPSSavings(numBackends.get(), rateArg) > minimumPPS).
            sorted((o1, o2) -> Double.compare(o2.getRawPPSSavings(), o1.getRawPPSSavings())).
            limit(maxRecommendations).
            collect(Collectors.toList());
        if (candidates.isEmpty()) continue;
        double savingsPPS = candidates.stream().
            mapToDouble(s -> s.getPPSSavings(numBackends.get(), rateArg)).
            sum();
        System.out.println("Confidence: " + ((1.0 - hypothesisManager.getConfidence()) * 100) + "%");
        System.out.println("Hypothesis Tracked: " + results.size() + " - potential total savings: " +
            savingsPPS + "pps");
        int index = 1;
        for (Hypothesis hypothesis : candidates) {
          double hConfidence = 100.0 - 100 * hypothesis.getViolationPercentage();
          double hSavings = hypothesis.getPPSSavings(numBackends.get(), rateArg);
          System.out.println("Action " + index + ": " + hypothesis.getDescription() +
              " (Savings: " + hSavings + "pps / Confidence: " + hConfidence + "%)");
          index++;
        }
      }
      System.out.flush();
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.incrementAge();
      }
    }
  }

  private void bidirectionalProcessHypothesis(double confidence, HypothesisManager previous,
                                              List<Hypothesis> toAdd, HypothesisManager hypothesisManager) {
    for (Hypothesis h : toAdd) {
      hypothesisManager.addHypothesis(h);
    }
    toAdd.clear();
    List<Hypothesis> rejected = hypothesisManager.removeAllViolatingHypothesis(confidence);
    for (Hypothesis h : rejected) {
      if (h.getViolationPercentage() <= confidence) {
        if (previous != null) {
          previous.addHypothesis(h);
        }
      } else {
        toAdd.add(h);
      }
    }
  }

  public void consumeReportPoint(boolean accessed, String metric, String host, Multimap<String, String> pointTags,
                                 long timestamp, double value) {
    for (HypothesisManager hm : managers) {
      if (hm.consumeReportPoint(accessed, metric, host, pointTags, timestamp, value, maxRecommendations)) {
        break;
      }
    }
  }

  public void offerHypothesis(Hypothesis hypothesis) {
    if (hypothesisDedupCache.getIfPresent(hypothesis) != null) {
      return;
    }
    hypothesisDedupCache.put(hypothesis, true);
    for (HypothesisManager hm : managers) {
      // offer to the first tier that can take it.
      if (hm.offerHypothesis(hypothesis)) {
        break;
      }
    }
  }
}
