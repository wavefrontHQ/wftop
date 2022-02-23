package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.components.MurmurHash3;
import net.agkn.hll.HLL;

import java.util.List;
import java.util.Map;

public class SeriesTrackingHypothesisDelegate implements Hypothesis {
  private final Hypothesis delegate;
  private final HLL cardinality = new HLL(13, 5);
  private final MurmurHash3.LongPair longPair = new MurmurHash3.LongPair();

  public SeriesTrackingHypothesisDelegate(Hypothesis hypothesis) {
    this.delegate = hypothesis;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public List<String> getDimensions() {
    return delegate.getDimensions();
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return delegate.cloneHypothesis();
  }

  @Override
  public double getRawPPSSavingsRate(boolean lifetime) {
    return delegate.getRawPPSSavingsRate(lifetime);
  }

  @Override
  public double getInstaneousRate() {
    return delegate.getInstaneousRate();
  }

  @Override
  public double getViolationPercentage() {
    return delegate.getViolationPercentage();
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host, Multimap<String, String> pointTags, long timestamp, double value) {
    boolean retVal = delegate.processReportPoint(accessed, metric, host, pointTags, timestamp, value);
    if (retVal) {

      Multimap<String, String> cloned = ImmutableMultimap.<String, String>builder().
          putAll(pointTags).put("source", host).build();
      longPair.val1 = 0;
      longPair.val2 = 0;
      byte[] b;
      b = metric.getBytes();
      MurmurHash3.murmurhash3_x64_128(b, 0, b.length, (int) longPair.val1, longPair);

      for (Map.Entry<String, String> entry : cloned.entries()) {
        b = entry.getKey().getBytes();
        MurmurHash3.murmurhash3_x64_128(b, 0, b.length, (int) longPair.val1, longPair);
        b = entry.getValue().getBytes();
        MurmurHash3.murmurhash3_x64_128(b, 0, b.length, (int) longPair.val1, longPair);
      }
      cardinality.addRaw(longPair.val1);
    }

    return retVal;
  }

  public long getEstimatedTotalSeriesCount() {
    return cardinality.cardinality();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public int getAge() {
    return delegate.getAge();
  }

  @Override
  public void incrementAge() {
    delegate.incrementAge();
  }

  @Override
  public void resetAge() {
    delegate.resetAge();
  }
}
