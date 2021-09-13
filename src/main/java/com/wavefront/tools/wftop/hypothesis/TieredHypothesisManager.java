package com.wavefront.tools.wftop.hypothesis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class manages a collection of {@link HypothesisManager} that's differed by
 * confidence/error rate.  {@link Hypothesis} that fails one level will be passed down
 * to a less strict level via confidences.
 */
public class TieredHypothesisManager {

  private static final Logger log = Logger.getLogger(TieredHypothesisManager.class.getCanonicalName());

  private final List<HypothesisManager> managers = new ArrayList<>();
  private final PointsSpy spy;
  private final int maxRecommendations;
  private final Cache<Hypothesis, Boolean> hypothesisDedupCache = Caffeine.newBuilder().
      expireAfterWrite(5, TimeUnit.MINUTES).
      expireAfterAccess(5, TimeUnit.MINUTES).
      build();
  private final AtomicBoolean ignore = new AtomicBoolean(true);
  private final long generationTime;

  private final long usageLookbackDays;
  private final double usageFPPRate;

  /**
   * @param confidences or margin of error
   */
  public TieredHypothesisManager(int maxHypothesis, long generationTime, PointsSpy pointsSpy,
                                 int maxRecommendations, long usageLookbackDays, double usageFPPRate,
                                 double... confidences) {
    this.generationTime = generationTime;
    this.spy = pointsSpy;
    this.maxRecommendations = maxRecommendations;
    this.usageLookbackDays = usageLookbackDays;
    this.usageFPPRate = usageFPPRate;

    double[] confidencesCopy = Arrays.copyOf(confidences, confidences.length);
    for (double confidence : confidencesCopy) {
      managers.add(new HypothesisManager(maxHypothesis, confidence, usageLookbackDays, usageFPPRate));
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
            // if saving is greater than minPPS
                filter(s -> s.getPPSSavings(s.getAge() > 10, numBackends.get(), rateArg) > minimumPPS).
            // sort by reporting rate
                sorted((o1, o2) -> Double.compare(o2.getRawPPSSavingsRate(o2.getAge() > 10),
                o1.getRawPPSSavingsRate(o1.getAge() > 10))).
            limit(maxRecommendations).
            collect(Collectors.toList());
        // total saving of this hypothesis manager
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
          double hConfidence = 100.0 - 100 * hypothesis.getViolationPercentage(usageLookbackDays, usageFPPRate);
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

  private void bidirectionalProcessHypothesis(double prevConfidence, HypothesisManager previous,
                                              List<Hypothesis> toAdd, HypothesisManager hypothesisManager) {
    for (Hypothesis h : toAdd) {
      hypothesisManager.addHypothesis(h);
    }
    toAdd.clear();
    HypothesisEvalResult result = hypothesisManager.removeAllViolatingHypothesis(prevConfidence);

    if (previous != null) {
      result.getWithinBounds().forEach(previous::addHypothesis);
    }
    toAdd.addAll(result.getRejected());
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
