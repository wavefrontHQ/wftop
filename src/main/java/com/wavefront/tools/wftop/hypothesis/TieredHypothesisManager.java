package com.wavefront.tools.wftop.hypothesis;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  public void run(TieredHMAsset asset) throws InterruptedException {
    long start = System.currentTimeMillis();
    int fileWrite = 1;
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
        List<Hypothesis> candidates = getRecommendations(asset, results);
        // total saving of this hypothesis manager
        double savingsPPS = getSavingsPPS(asset, candidates);
        System.out.println(MessageFormat.format(
            "Confidence: {0}% / Hypothesis Tracked: {1} / Hypothesis Rejected: {2}",
            (1.0 - hypothesisManager.getConfidence()) * 100, results.size(),
            hypothesisManager.getBlacklistedHypothesis()));
        System.out.println(MessageFormat.format("Potential Savings: {0,number,#.##}pps", savingsPPS));
        int index = 1;
        for (Hypothesis hypothesis : candidates) {
          double hConfidence = 100.0 - 100 * hypothesis.getViolationPercentage(usageLookbackDays, usageFPPRate);
          double fifteenMinuteSavings = hypothesis.getPPSSavings(
              false, asset.getNumBackends().get(), asset.getRateArg());
          double lifetimeSavings = hypothesis.getPPSSavings(
              true, asset.getNumBackends().get(), asset.getRateArg());
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
      long now = System.currentTimeMillis();
      if (asset.getWriteSignal().get() ||
          (now - start) >= asset.getOutputGenerationTime()) {
        start = now;
        writeToExcel(managers, asset, fileWrite);
        fileWrite++;
      }
      for (HypothesisManager hypothesisManager : managers) {
        hypothesisManager.incrementAge();
      }
    }
  }

  private double getSavingsPPS(TieredHMAsset asset, List<Hypothesis> candidates) {
    return candidates.stream().
        mapToDouble(s -> s.getPPSSavings(
            s.getAge() > 10,
            asset.getNumBackends().get(),
            asset.getRateArg())).
        sum();
  }

  private List<Hypothesis> getRecommendations(TieredHMAsset asset, List<Hypothesis> results) {
    return results.stream().
        filter(s -> s.getAge() > 1).
        // if saving is greater than minPPS
            filter(s -> s.getPPSSavings(s.getAge() > 10,
            asset.getNumBackends().get(), asset.getRateArg()) > asset.getMinimumPPS()).
        // sort by reporting rate
            sorted((o1, o2) -> Double.compare(o2.getRawPPSSavingsRate(o2.getAge() > 10),
            o1.getRawPPSSavingsRate(o1.getAge() > 10))).
        limit(maxRecommendations).
        collect(Collectors.toList());
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

  public void writeToExcel(List<HypothesisManager> managers, TieredHMAsset asset, int fileWrite) {
    if (StringUtils.isEmpty(asset.getOutputFile())) {
      log.info("No output file provided.");
      return;
    } else {
      log.info("Begin writing to output: " + asset.getOutputFile());
    }
    asset.getWriteSignal().set(false);

    XSSFWorkbook workbook = new XSSFWorkbook();
    XSSFSheet sheet = workbook.createSheet("Cost Saving Analysis");
    List<String> cellTitles = Lists.newArrayList(
        "Timeseries", "PPS (15m)", "PPS (mean)", "Confidence", "TTL", "Additional Info");

    int rowInd = 0;
    Row header = sheet.createRow(rowInd++);
    int cellInd = 0;
    for (String title : cellTitles) {
      Cell headerCell = header.createCell(cellInd);
      headerCell.setCellValue(title);
      cellInd++;
    }

    for (HypothesisManager hypothesisManager : managers) {
      if (hypothesisManager.getConfidence() >= 1.0) continue; // ignore the catch-all hypothesis.
      log.info("Writing confidence " + hypothesisManager.getConfidence());
      List<Hypothesis> results = hypothesisManager.getAllHypothesis();
      List<Hypothesis> candidates = getRecommendations(asset, results);

      Cell cell;
      for (Hypothesis hypothesis : candidates) {
        double hConfidence = 100.0 - 100 * hypothesis.getViolationPercentage(usageLookbackDays, usageFPPRate);
        double fifteenMinuteSavings = hypothesis.getPPSSavings(
            false, asset.getNumBackends().get(), asset.getRateArg());
        double lifetimeSavings = hypothesis.getPPSSavings(
            true, asset.getNumBackends().get(), asset.getRateArg());

        Row row = sheet.createRow(rowInd++);
        // "Timeseries", "PPS (15m)", "PPS (mean)", "Confidence", "TTL", "Additional Info");
        List<String> strings = hypothesis.getDimensions();
        String series = String.join(", ", strings);
        cell = row.createCell(0);
        cell.setCellValue(series);
        cell = row.createCell(1);
        cell.setCellValue(fifteenMinuteSavings);
        cell = row.createCell(2);
        cell.setCellValue(lifetimeSavings);
        cell = row.createCell(3);
        cell.setCellValue(hConfidence);
        cell = row.createCell(4);
        cell.setCellValue(Duration.ofMillis(hypothesis.getAge() * 2 * generationTime).toString());
        cell = row.createCell(5);
        cell.setCellValue(hypothesis.getDescription());
      }
    }
    String name = MessageFormat.format(
        "{0}_{1, number, integer}.xlsx", asset.getOutputFile(), fileWrite);
    try (FileOutputStream out = new FileOutputStream(name)) {
      workbook.write(out);
    } catch (IOException e) {
      log.warning("Unable to write to file " + name);
    }

    log.info("Done output.");
  }
}
