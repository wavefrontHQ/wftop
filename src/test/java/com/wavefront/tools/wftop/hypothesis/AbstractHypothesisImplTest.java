package com.wavefront.tools.wftop.hypothesis;

import org.junit.Test;

import static org.junit.Assert.*;

public class AbstractHypothesisImplTest {

  @Test
  public void test() {
    long bigLong = 129_600_000_000L;
    long smallViolation = 1000l;

    double violation = (double) smallViolation / bigLong;


    double expectedAccuracy = Math.pow(1 - 0.01, 15);
    double confidence = (1.0 - violation);

    System.out.println(violation);
    System.out.println(expectedAccuracy);
    System.out.println(confidence >= expectedAccuracy);

    System.out.println(1.0 - ((1.0 - violation) / expectedAccuracy));
  }


}