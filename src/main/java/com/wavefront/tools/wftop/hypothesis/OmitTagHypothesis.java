package com.wavefront.tools.wftop.hypothesis;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Objects;

public class OmitTagHypothesis extends AbstractUsageDataFPPAdjustingHypothesisImpl {

  private final String tagK;
  private final String tagV;

  public OmitTagHypothesis(String tagK, String tagV) {
    this.tagK = tagK;
    this.tagV = tagV;
  }

  @Override
  public Hypothesis cloneHypothesis() {
    return new OmitTagHypothesis(tagK, tagV);
  }

  @Override
  public String getDescription() {
    return "Eliminate all unused metrics with the tag: \"" + tagK + "\"=\"" + tagV + "\"";
  }

  @Override
  public List<String> getDimensions() {
    return Lists.newArrayList("", tagK + " = " + tagV);
  }

  @Override
  public boolean processReportPoint(boolean accessed, String metric, String host,
                                    Multimap<String, String> pointTags, long timestamp, double value) {
    if (pointTags.containsKey(tagK) && pointTags.get(tagK).equals(tagV)) {
      hits.incrementAndGet();
      rate.mark();
      instancenousRate.mark();
      if (accessed) {
        violations.incrementAndGet();
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OmitTagHypothesis that = (OmitTagHypothesis) o;
    return tagK.equals(that.tagK) &&
        tagV.equals(that.tagV);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tagK, tagV);
  }
}
