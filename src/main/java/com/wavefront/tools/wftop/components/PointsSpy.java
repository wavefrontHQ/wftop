package com.wavefront.tools.wftop.components;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.wavefront.ingester.AbstractIngesterFormatter;
import com.wavefront.ingester.ReportPointIngesterFormatter;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.protocol.HttpContext;
import wavefront.report.ReportPoint;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Component that connects via HTTPS to a Wavefront cluster, establishes the connection and attempts to spy with a set
 * of parameters.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class PointsSpy {

  private static final Logger log = Logger.getLogger(PointsSpy.class.getCanonicalName());

  /**
   * Ingestion has a weird format that's not like our ingestion unfortunately.
   */
  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      ReportPointIngesterFormatter.newBuilder().
          whiteSpace().appendMetricName().
          whiteSpace().appendBoundedAnnotationsConsumer().
          whiteSpace().appendTimestamp(TimeUnit.MILLISECONDS).
          whiteSpace().appendValue().
          whiteSpace().appendAnnotationsConsumer().
          whiteSpace().build();

  private final AtomicBoolean connected = new AtomicBoolean(false);

  private final CloseableHttpAsyncClient httpAsyncClient;
  private Future<?> inflightCall;

  private String clusterUrl;
  private String token;
  /**
   * Number of days to use an threshold for usage information.
   */
  private int usageDaysThreshold = 7;
  /**
   * Metric prefix to further cull the data set.
   */
  private String metricPrefix = "";
  /**
   * Host prefix to further cull the data set.
   */
  private String hostPrefix = "";
  /**
   * Point tag keys that must be present on the returned data.
   */
  private Set<String> pointTagKeys = Collections.emptySet();
  /**
   * Sampling rate, must be > 0 and <= 1.
   */
  private double samplingRate = 0.01;

  private Listener listener;
  private StringBuilder curr = new StringBuilder();

  public PointsSpy() {
    this.httpAsyncClient = HttpAsyncClients.custom().setDefaultRequestConfig(
        RequestConfig.custom().
            setConnectionRequestTimeout(10_000).
            setSocketTimeout(10_000).
            setConnectTimeout(10_000).build()).
        build();
    this.httpAsyncClient.start();
  }

  public void setSamplingRate(double samplingRate) {
    this.samplingRate = samplingRate;
  }

  public void setUsageDaysThreshold(int threshold) {
    this.usageDaysThreshold = threshold;
  }

  /**
   * Set the spy with new parameters, if spying is already in flight, we'll restart the spy.
   *
   * @param metricPrefix       Metric prefix (e.g. com.wavefront)
   * @param hostPrefix         Host prefix (e.g. metrics-2a)
   * @param pointTagKeys       Point tag keys to include for points spied.
   * @param samplingRate       Sampling rate between 0 (exclusive) and 1 (inclusive).
   * @param usageDaysThreshold Usage days threshold (between 1 and 60).
   */
  public void setParameters(String clusterUrl, String token,
                            @Nullable String metricPrefix, @Nullable String hostPrefix,
                            @Nullable Set<String> pointTagKeys,
                            double samplingRate, int usageDaysThreshold) {
    if (samplingRate <= 0 || samplingRate > 1) {
      throw new IllegalArgumentException("Cannot set sampling rate to <= 0 or > 1");
    }
    if (usageDaysThreshold < 1 || usageDaysThreshold > 60) {
      throw new IllegalArgumentException("Cannot set usage threshold (in days) to < 1 or > 60");
    }
    this.clusterUrl = clusterUrl;
    this.token = token;
    this.metricPrefix = Strings.nullToEmpty(metricPrefix);
    this.hostPrefix = Strings.nullToEmpty(hostPrefix);
    this.pointTagKeys = pointTagKeys == null ? Collections.emptySet() : ImmutableSet.copyOf(pointTagKeys);
    this.samplingRate = samplingRate;
    this.usageDaysThreshold = usageDaysThreshold;
  }

  public String getClusterUrl() {
    return clusterUrl;
  }

  public String getToken() {
    return token;
  }

  public int getUsageDaysThreshold() {
    return usageDaysThreshold;
  }

  public String getMetricPrefix() {
    return metricPrefix;
  }

  public String getHostPrefix() {
    return hostPrefix;
  }

  public Set<String> getPointTagKeys() {
    return pointTagKeys;
  }

  public double getSamplingRate() {
    return samplingRate;
  }

  /**
   * Start the spying stream (will invoke listeners). Stops the current stream if one is already in-flight (there will
   * only ever be one stream of points coming in from one connection.
   */
  public void start() {
    synchronized (this) {
      stop();
      if (this.listener != null) {
        this.listener.onConnecting(this);
      }
      String spyUrl = getSpyUrl();
      HttpGet httpGet = new HttpGet(spyUrl);
      log.log(Level.INFO, "Starting spy request: " + spyUrl);
      httpGet.setHeader("Authorization", "Bearer " + token);
      this.inflightCall = this.httpAsyncClient.execute(HttpAsyncMethods.create(httpGet),
          new AsyncCharConsumer<Boolean>() {

            @Override
            protected void onResponseReceived(HttpResponse response) {
              if (response.getStatusLine().getStatusCode() != 200) {
                if (listener != null) {
                  listener.onConnectivityChanged(PointsSpy.this, false,
                      response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
                }
              } else {
                log.info("Spy request established");
                if (!connected.getAndSet(true)) {
                  if (listener != null) {
                    listener.onConnectivityChanged(PointsSpy.this, true, null);
                  }
                }
              }

            }

            @Override
            protected Boolean buildResult(HttpContext context) {
              // we don't have a "result" per-say.
              // if the stream ever finishes, we are no longer connected (the server enforces a time limit).
              if (connected.getAndSet(false)) {
                if (listener != null) {
                  listener.onConnectivityChanged(PointsSpy.this, false, "Stream Interrupted");
                }
              }
              return null;
            }

            @Override
            protected void onCharReceived(CharBuffer buf, IOControl ioctrl) throws IOException {
              while (buf.hasRemaining()) {
                char c = buf.get();
                if (c == '\n' || c == '\r') {
                  if (curr.length() != 0) {
                    try {
                      handleLine(curr.toString());
                    } catch (Exception ex) {
                      log.log(Level.WARNING, "Failed to process line: " + curr.toString(), ex);
                    }
                    curr.setLength(0);
                  }
                } else {
                  curr.append(c);
                }
              }
            }
          },
          new FutureCallback<>() {
            @Override
            public void completed(Boolean result) {
              log.warning("Spy request completed (will reconnect)");
              httpGet.abort();
            }

            @Override
            public void failed(Exception ex) {
              log.log(Level.WARNING, "Spy request failed (will reconnect)", ex);
              httpGet.abort();
              if (PointsSpy.this.listener != null) {
                PointsSpy.this.listener.onConnectivityChanged(PointsSpy.this, false, "DISCONNECTED: " + ex.getMessage());
              }
              start();
            }

            @Override
            public void cancelled() {
              httpGet.abort();
            }
          });
    }
  }

  public void stop() {
    synchronized (this) {
      if (this.inflightCall != null) {
        log.log(Level.WARNING, "Stopping spy request");
        this.connected.set(false);
        this.inflightCall.cancel(true);
        this.inflightCall = null;
        if (this.listener != null) {
          this.listener.onConnectivityChanged(this, false, null);
        }
      }
    }
  }

  private String getSpyUrl() {
    URIBuilder builder = new URIBuilder();
    builder.setScheme("https").setHost(clusterUrl).setPath("/api/spy/points");
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("usage", "true"));
    params.add(new BasicNameValuePair("sampling", String.valueOf(samplingRate)));
    params.add(new BasicNameValuePair("includeScalingFactor", "true"));
    params.add(new BasicNameValuePair("metric", metricPrefix));
    params.add(new BasicNameValuePair("host", hostPrefix));
    params.add(new BasicNameValuePair("usageThresholdDays", String.valueOf(usageDaysThreshold)));
    if (!pointTagKeys.isEmpty()) {
      params.add(new BasicNameValuePair("pointTagKey", Joiner.on(",").join(pointTagKeys)));
    }
    builder.setParameters(params);
    return builder.toString();
  }

  public void setListener(Listener listener) {
    this.listener = listener;
  }

  public boolean isConnected() {
    return connected.get();
  }

  private void handleLine(String line) {
    line = line.trim();
    if (line.startsWith("# backends: ")) {
      try {
        int backends = Integer.parseInt(line.substring(12));
        if (listener != null) {
          this.listener.onBackendCountChanges(this, backends);
        }
      } catch (NumberFormatException ex) {
        log.warning("Cannot parse backends line: " + line);
      }
    } else if (line.startsWith("[UNACCESSED] ")) {
      parseMetric(false, line.substring(13));
    } else if (line.startsWith("[ACCESSED]   ")) {
      parseMetric(true, line.substring(13));
    } else {
      // for old wavefront clusters, no usage information is returned.
      try {
        parseMetric(false, line);
      } catch (Throwable t) {
        // ignored.
      }
    }
  }

  private void parseMetric(boolean accessed, String line) {
    ReportPoint drive = FORMAT.drive(line, null, null);
    if (listener != null) {
      Multimap<String, String> annotations;
      if (drive.getAnnotations() != null) {
        annotations = ArrayListMultimap.create(drive.getAnnotations().size(), 1);
        for (Map.Entry<String, String> annotation : drive.getAnnotations().entrySet()) {
          annotations.put(annotation.getKey(), annotation.getValue());
        }
      } else {
        annotations = ImmutableMultimap.of();
      }
      this.listener.onMetricReceived(this, accessed, drive.getMetric(), drive.getHost(), annotations,
          drive.getTimestamp(), (Double) drive.getValue());
    }
  }

  public interface Listener {

    void onBackendCountChanges(PointsSpy pointsSpy, int numBackends);

    void onMetricReceived(PointsSpy pointsSpy, boolean accessed, String metric, String host,
                          Multimap<String, String> pointTags, long timestamp, double value);

    void onConnectivityChanged(PointsSpy pointsSpy, boolean connected, @Nullable String message);

    void onConnecting(PointsSpy pointsSpy);
  }
}
