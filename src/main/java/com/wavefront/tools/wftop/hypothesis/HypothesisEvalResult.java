package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Lists;

import java.util.List;

public class HypothesisEvalResult {
  List<Hypothesis> rejected;
  List<Hypothesis> withinBounds;

  public HypothesisEvalResult(List<Hypothesis> rejected, List<Hypothesis> withinBounds) {
    this.rejected = rejected;
    this.withinBounds = withinBounds;
  }

  public static HypothesisEvalResult newHypothesisEvalResult() {
    return new HypothesisEvalResult(Lists.newArrayList(), Lists.newArrayList());
  }

  public List<Hypothesis> getRejected() {
    return rejected;
  }

  public List<Hypothesis> getWithinBounds() {
    return withinBounds;
  }
}
