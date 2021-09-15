package com.wavefront.tools.wftop.hypothesis;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@AllArgsConstructor
public class TieredHMAsset {
  private final AtomicInteger numBackends;
  private final double minimumPPS;
  private final double rateArg;
  private final AtomicBoolean writeSignal;
  private final String outputFile;
  private final long outputGenerationTime;

}
