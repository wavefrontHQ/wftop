package com.wavefront.tools.wftop.hypothesis;

import com.codahale.metrics.Meter;
import com.wavefront.tools.wftop.hypothesis.pojo.ViolationResult;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractHypothesisImpl implements Hypothesis {

  protected Meter rate = new Meter();
  protected Meter instancenousRate = new Meter();
  protected final AtomicLong hits = new AtomicLong();
  protected final AtomicLong violations = new AtomicLong();
  protected final AtomicInteger age = new AtomicInteger();

  @Override
  public int getAge() {
    return age.get();
  }

  @Override
  public void incrementAge() {
    age.incrementAndGet();
  }

  @Override
  public void resetAge() {
    age.set(0);
  }

  @Override
  public double getRawPPSSavingsRate(boolean lifetime) {
    if (lifetime) return rate.getMeanRate();
    return rate.getFifteenMinuteRate();
  }

  @Override
  public double getInstaneousRate() {
    return instancenousRate.getOneMinuteRate();
  }

  @Override
  public double getViolationPercentage() {
    return (double) violations.get() / hits.get();
  }

  @Override
  public void reset() {
    instancenousRate = new Meter();
  }
}
