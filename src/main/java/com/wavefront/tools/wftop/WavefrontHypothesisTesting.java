package com.wavefront.tools.wftop;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.PointsSpy;
import com.wavefront.tools.wftop.components.Type;
import com.wavefront.tools.wftop.hypothesis.*;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class WavefrontHypothesisTesting {

  private static Logger log = Logger.getLogger(WavefrontHypothesisTesting.class.getCanonicalName());

  @Parameter(names = "--token", description = "Wavefront Token")
  private String token = null;

  @Parameter(names = "--cluster", description = "Wavefront Cluster (metrics.wavefront.com)")
  private String cluster = null;

  @Parameter(names = {"-r", "-rate"}, description = "Sample rate")
  private double rateArg = 0.01;

  @Parameter(names = {"-sep", "-separators"}, description = "Separators")
  private String separatorsArg = ".-_=";

  @Parameter(names = {"-m", "-minPPS"}, description = "Minimum PPS savings to consider")
  private double minimumPPS = 1000;

  @Parameter(names = {"-recommendations"}, description = "Number of recommendations per tier")
  private int recommendations = 25;

  public static void main(String[] args) {
    WavefrontHypothesisTesting hypothesisTesting = new WavefrontHypothesisTesting();
    JCommander jCommander = JCommander.newBuilder().addObject(hypothesisTesting).build();
    try {
      jCommander.parse(args);
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
    AtomicInteger numBackends = new AtomicInteger(1);
    TieredHypothesisManager tieredHypothesisManager = new TieredHypothesisManager(10000, spy, recommendations,
        0.001, 0.02, 0.05, 0.10, 0.20, 1.0);
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
        if (!accessed) {
          tieredHypothesisManager.offerHypothesis(new OmitExactMetricHypothesis(metric));
          for (Map.Entry<String, String> entry : pointTags.entries()) {
            if (entry.getKey().equals("_wavefront_source")) continue;
            tieredHypothesisManager.offerHypothesis(new OmitTagHypothesis(entry.getKey(), entry.getValue()));
            tieredHypothesisManager.offerHypothesis(new OmitMetricAndTagHypothesis(metric, entry.getKey(),
                entry.getValue()));
          }
          tieredHypothesisManager.offerHypothesis(new OmitMetricAndTagHypothesis(metric, "source", host));
          StringBuilder sourceSB = new StringBuilder();
          for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            sourceSB.append(c);
            if (separatorsArg.contains(String.valueOf(c))) {
              tieredHypothesisManager.offerHypothesis(new OmitHostPrefixAndMetricHypothesis(sourceSB.toString(),
                  metric));
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
        if (!connected) {
          pointsSpy.start();
        }
      }

      @Override
      public void onConnecting(PointsSpy pointsSpy) {
        log.info("Connecting...");
      }
    });
    tieredHypothesisManager.run(numBackends, minimumPPS, rateArg);
  }
}
