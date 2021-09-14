package com.wavefront.tools.wftop;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;
import com.wavefront.tools.wftop.components.Type;
import com.wavefront.tools.wftop.hypothesis.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class WavefrontHypothesisTesting {

  private static Logger log = Logger.getLogger(WavefrontHypothesisTesting.class.getCanonicalName());
  private static List<String> STOP_COMMANDS = Lists.newArrayList("stop", "s", "quit", "q");
  private static final AtomicBoolean stopSignal = new AtomicBoolean(false);

  @Parameter(names = "--token", description = "Wavefront Token")
  private String token = null;

  @Parameter(names = "--cluster", description = "Wavefront Cluster (metrics.wavefront.com)")
  private String cluster = null;

  @Parameter(names = {"-r", "-rate"}, description = "Sample rate")
  private double rateArg = 0.01;

  @Parameter(names = {"-days"}, description = "Days to look back for access data")
  private int usageDaysLookback = 7;

  @Parameter(names = {"-usageFPP"}, description = "False positive rate of usage data")
  private double usageFPPRate = 0.01;

  @Parameter(names = {"-sep", "-separators"}, description = "Separators")
  private String separatorsArg = ".-_=";

  @Parameter(names = {"-m", "-minPPS"}, description = "Minimum PPS savings to consider")
  private double minimumPPS = 1000;

  @Parameter(names = {"-recommendations"}, description = "Number of recommendations per tier")
  private int recommendations = 50;

  @Parameter(names = {"-generationTime"}, description = "Time for each generation (at least one minute)")
  private long generationTime = 60000;

  @Parameter(names = {"-csvOutput"}, description = "File name for CSV output")
  private String excelOutput = "";


  public static void main(String[] args) {
    WavefrontHypothesisTesting hypothesisTesting = new WavefrontHypothesisTesting();
    JCommander jCommander = JCommander.newBuilder().addObject(hypothesisTesting).build();
    try {
      jCommander.parse(args);
      getStartSignal(stopSignal);
      hypothesisTesting.run();
    } catch (ParameterException | InterruptedException pe) {
      System.out.println("ParameterException: " + pe.getMessage());
      System.out.println("Run ./target/wftop with -h or --help to view flag options");
      System.exit(1);
    }

  }

  public void run() throws InterruptedException {
    PointsSpy spy = new PointsSpy();
    spy.setParameters(cluster, token, null, null, rateArg);
    spy.setUsageDaysThreshold(usageDaysLookback);
    AtomicInteger numBackends = new AtomicInteger(1);
    TieredHypothesisManager tieredHypothesisManager = new TieredHypothesisManager(1000, generationTime, spy,
        recommendations, usageDaysLookback, usageFPPRate, 0.001, 0.02, 0.05, 0.10, 0.20, 1.0);
    spy.setListener(new PointsSpy.Listener() {
      @Override
      public void onBackendCountChanges(PointsSpy pointsSpy, int backends) {
        numBackends.set(backends);
      }

      @Override
      public void onIdReceived(PointsSpy pointsSpy, Type type, @Nullable String name) {

      }

      @Override
      public void onMetricReceived(PointsSpy pointsSpy, boolean accessed, String metric, String host,
                                   Multimap<String, String> pointTags, long timestamp, double value) {
        tieredHypothesisManager.consumeReportPoint(accessed, metric, host, pointTags, timestamp, value);
        // always attempt a constant hypothesis (hm will reject if already blacklisted).
        tieredHypothesisManager.offerHypothesis(
            new MetricIsConstantHypothesis(metric, 10000));
        // attempt a stale metric hypothesis if we see a metric that's old
        if (timestamp < System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) {
          tieredHypothesisManager.offerHypothesis(new MetricIsAlwaysOldHypothesis(metric));
        }
        // if a metric is not accessed, attempt a whole series of hypothesis.
        if (!accessed) {
          tieredHypothesisManager.offerHypothesis(new OmitExactMetricHypothesis(metric));
          for (Map.Entry<String, String> entry : pointTags.entries()) {
            if (entry.getKey().equals("_wavefront_source")) continue;
            tieredHypothesisManager.offerHypothesis(new OmitTagHypothesis(entry.getKey(), entry.getValue()));
            tieredHypothesisManager.offerHypothesis(new OmitMetricAndTagHypothesis(metric, entry.getKey(),
                entry.getValue()));
          }
          tieredHypothesisManager.offerHypothesis(
              new OmitMetricAndTagHypothesis(metric, "source", host));
          StringBuilder sourceSB = new StringBuilder();
          for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            sourceSB.append(c);
            if (separatorsArg.contains(String.valueOf(c))) {
              tieredHypothesisManager.offerHypothesis(
                  new OmitHostPrefixAndMetricHypothesis(sourceSB.toString(), metric));
            }
          }
          StringBuilder metricSB = new StringBuilder();
          for (int i = 0; i < metric.length(); i++) {
            char c = metric.charAt(i);
            metricSB.append(c);
            if (separatorsArg.contains(String.valueOf(c))) {
              tieredHypothesisManager.offerHypothesis(new OmitMetricPrefixHypothesis(metricSB.toString()));
              for (Map.Entry<String, String> entry : pointTags.entries()) {
                if (entry.getKey().equals("_wavefront_source")) continue;
                tieredHypothesisManager.offerHypothesis(new OmitMetricPrefixAndTagHypothesis(metricSB.toString(),
                    entry.getKey(),
                    entry.getValue()));
              }
              tieredHypothesisManager.offerHypothesis(new OmitMetricPrefixAndTagHypothesis(metricSB.toString(),
                  "source", host));
            }
          }
        }
      }

      @Override
      public void onConnectivityChanged(PointsSpy pointsSpy, boolean connected, @Nullable String message) {
        log.info("Connectivity Changed, connected: " + connected + ": " + message);
      }

      @Override
      public void onConnecting(PointsSpy pointsSpy) {
        log.info("Connecting...");
      }
    });

    TieredHMAsset tieredHMAsset =
        new TieredHMAsset(numBackends, minimumPPS, rateArg, stopSignal, excelOutput);
    tieredHypothesisManager.run(tieredHMAsset);
  }

  public static void getStartSignal(AtomicBoolean stopSignal) {
    new Thread(() -> {
      Scanner scanner = new Scanner(System.in);
      while (!stopSignal.get() && scanner.hasNextLine()) {
        String line = scanner.nextLine();
        for (String stop : STOP_COMMANDS) {
          if (stop.equals(line)) {
            log.info("Running last collection.");
            stopSignal.set(true);
          }
        }
      }
    }).start();
  }
}
