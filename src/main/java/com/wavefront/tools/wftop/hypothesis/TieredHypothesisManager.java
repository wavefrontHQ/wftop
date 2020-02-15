package com.wavefront.tools.wftop.hypothesis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final AtomicBoolean ignore = new AtomicBoolean(true);
  private final long generationTime;

  public TieredHypothesisManager(int maxHypothesis, long generationTime, PointsSpy pointsSpy,
                                 int maxRecommendations, double... confidences) {
    this.generationTime = generationTime;
    this.spy = pointsSpy;
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
    ignore.set(false);
    while (true) {
      if (!spy.isConnected()) {
        log.info("Reconnecting...");
        spy.start();
        Thread.sleep(10000);
      }
      log.info("Evaluating all hypothesis to collect non-cumulative PPS data");
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.evaluateAllHypothesis();
        hypothesisManager.sortHypothesisByInstantaneousRate();
        hypothesisManager.resetInstantaneousRateForAllHypothesis();
        hypothesisManager.resetDroppedCounter();
      }
      Thread.sleep(generationTime);
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
      Thread.sleep(generationTime);
      log.info("Trimming recommendations");
      confidence = -1;
      previous = null;
      for (HypothesisManager hypothesisManager : managers) {
        bidirectionalProcessHypothesis(confidence, previous, toAdd, hypothesisManager);
        hypothesisManager.trimLowPPSHypothesis(maxRecommendations);
        confidence = hypothesisManager.getConfidence();
        previous = hypothesisManager;
      }
      for (HypothesisManager hypothesisManager : managers) {
        if (hypothesisManager.getConfidence() >= 1.0) continue; // ignore the catch-all hypothesis.
        List<Hypothesis> results = hypothesisManager.getAllHypothesis();
        List<Hypothesis> candidates = results.stream().
            filter(s -> s.getAge() > 1).
            filter(s -> s.getPPSSavings(s.getAge() > 10, numBackends.get(), rateArg) > minimumPPS).
            sorted((o1, o2) -> Double.compare(o2.getRawPPSSavings(o2.getAge() > 10),
                o1.getRawPPSSavings(o1.getAge() > 10))).
            limit(maxRecommendations).
            collect(Collectors.toList());
        double savingsPPS = candidates.stream().
            mapToDouble(s -> s.getPPSSavings(s.getAge() > 10, numBackends.get(), rateArg)).
            sum();
        System.out.println(MessageFormat.format(
            "Confidence: {0}% / Hypothesis Tracked: {1} / Hypothesis Rejected: {2}",
            (1.0 - hypothesisManager.getConfidence()) * 100, results.size(),
            hypothesisManager.getBlacklistedHypothesis()));
        System.out.println(MessageFormat.format("Potential Savings: {0,number,#.##}pps", savingsPPS));
        int index = 1;
        for (Hypothesis hypothesis : candidates) {
          double hConfidence = 100.0 - 100 * hypothesis.getViolationPercentage();
          double fifteenMinuteSavings = hypothesis.getPPSSavings(false, numBackends.get(), rateArg);
          double lifetimeSavings = hypothesis.getPPSSavings(true, numBackends.get(), rateArg);
          System.out.println(MessageFormat.format(
              "{0}. {1} (Savings: {2,number,#.##}pps (15m) / {3,number,#.##}pps (lifetime) " +
                  "/ Confidence: {4,number,#.##}% / TTL: {5})",
              index, hypothesis.getDescription(), fifteenMinuteSavings, lifetimeSavings,
              hConfidence, Duration.ofMillis(hypothesis.getAge() * 2 * generationTime).toString()));
          index++;
        }
        System.out.println();
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
    if (ignore.get()) return;
    for (HypothesisManager hm : managers) {
      if (hm.consumeReportPoint(accessed, metric, host, pointTags, timestamp, value, maxRecommendations)) {
        break;
      }
    }
  }

  public void offerHypothesis(Hypothesis hypothesis) {
    if (ignore.get()) return;
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
