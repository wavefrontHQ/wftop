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
  private final Cache<Hypothesis, Boolean> hypothesisDedupCache = Caffeine.newBuilder().
      expireAfterWrite(5, TimeUnit.MINUTES).build();

  public TieredHypothesisManager(int maxHypothesis, PointsSpy spy, double... confidences) {
    this.spy = spy;
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
        hypothesisManager.resetInstantaneousRateForAllHypothesis();
        hypothesisManager.resetDroppedCounter();
      }
      Thread.sleep(60000);
      log.info("Begin ranked hypothesis testing");
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.removeAllViolatingHypothesis();
        hypothesisManager.enableRankedHypothesisEvaluation();
        hypothesisManager.sortHypothesisByInstantaneousRate();
        hypothesisManager.resetInstantaneousRateForAllHypothesis();
        hypothesisManager.resetDroppedCounter();
      }
      Thread.sleep(60000);
      log.info("Trimming recommendations");
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.removeAllViolatingHypothesis();
        hypothesisManager.trimLowPPSHypothesis();
      }
      for (HypothesisManager hypothesisManager : managers) {
        List<Hypothesis> results = hypothesisManager.getAllHypothesis();
        double savingsPPS = results.stream().
            mapToDouble(s -> s.getPPSSavings(numBackends.get(), rateArg)).
            filter(s -> s > minimumPPS).
            sum();
        System.out.println("Confidence: " + ((1.0 - hypothesisManager.getConfidence()) * 100) + "%");
        System.out.println("Hypothesis Tracked: " + results.size() + " - potential total savings: " +
            savingsPPS + "pps");
        List<Hypothesis> allHypothesis = results.stream().
            filter(s -> s.getPPSSavings(numBackends.get(), rateArg) > minimumPPS).
            sorted((o1, o2) -> Double.compare(o2.getRawPPSSavings(), o1.getRawPPSSavings())).
            limit(25).
            collect(Collectors.toList());
        int index = 1;
        for (Hypothesis hypothesis : allHypothesis) {
          double confidence = 100.0 - 100 * hypothesis.getViolationPercentage();
          double savings = hypothesis.getPPSSavings(numBackends.get(), rateArg);
          System.out.println("Action " + index + ": " + hypothesis.getDescription() +
              " (Savings: " + savings + "pps / Confidence: " + confidence + "%)");
          index++;
        }
      }
      System.out.flush();
    }
  }

  public void consumeReportPoint(boolean accessed, String metric, String host, Multimap<String, String> pointTags,
                                 long timestamp, double value) {
    for (HypothesisManager hm : managers) {
      if (hm.consumeReportPoint(accessed, metric, host, pointTags, timestamp, value)) {
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
      hm.offerHypothesis(hypothesis);
    }
  }
}
