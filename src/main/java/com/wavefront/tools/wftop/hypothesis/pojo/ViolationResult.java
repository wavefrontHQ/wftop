package com.wavefront.tools.wftop.hypothesis.pojo;

import lombok.Data;

@Data
public class ViolationResult {
  private final double rawPercentage;
  private final double adjustedPercentage;
}
