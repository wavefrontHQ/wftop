package com.wavefront.tools.wftop.components;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link RootNode}
 *
 * @author Joanna Ko (kjoanna@vmware.com)
 */
public class RootNodeTest {
  private Dimension analysisDimension = Dimension.METRIC;
  private boolean groupByIngestionSource = false;
  private RootNode root = new RootNode("root");

  @Before
  public void setUp() {
    root.reset();
  }

  /**
   * Adds points to rootNode, 2 with _wavefront_source and 3 without
   */
  void addPointsNoSource() {
    Multimap<String, String> noSource =
        ImmutableMultimap.<String, String>builder().put("pointTagKey", "pointTagValue").build();
    for (int i = 2; i < 5; i++) {
      root.accept(analysisDimension, groupByIngestionSource, false, "metric" + i, "host" + i,
          noSource, 0, 0);
    }
  }

  void addPointsWithSource() {
    String WAVEFRONT_SOURCE = "_wavefront_source";
    //Send points with _wavefront_source tag key
    Multimap<String, String> hasSourceProxy =
        ImmutableMultimap.<String, String>builder().put(WAVEFRONT_SOURCE, "proxy::user-a01.test.com").build();
    Multimap<String, String> hasSourceToken =
        ImmutableMultimap.<String, String>builder().put(WAVEFRONT_SOURCE, "token::abcde-12345").put("testKey", "testValue").build();

    root.accept(analysisDimension, groupByIngestionSource, false, "metric", "host",
        hasSourceProxy, 0, 0);
    root.accept(analysisDimension, groupByIngestionSource, false, "metric1", "host1",
        hasSourceToken, 0, 0);
  }

  /**
   * Tests size of SourceNode map when not grouping by _wavefront_source tag.
   */
  @Test
  public void noGroupBy() {
    //root always has one default SourceNode: "None"
    assertEquals(1, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));

    //test all points grouped under "None"
    addPointsNoSource();
    assertEquals(1, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));

    addPointsWithSource();
    assertEquals(1, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));
  }

  /**
   * Tests size of SourceNode map when grouping by _wavefront_source tag.
   */
  @Test
  public void GroupBy() {
    this.groupByIngestionSource = true;
    addPointsNoSource();
    assertEquals(1, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));

    addPointsWithSource();
    assertEquals(3, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));
    assertTrue(root.getNodes().containsKey("proxy::user-a01.test.com"));
    assertTrue(root.getNodes().containsKey("token::abcde-12345"));
  }
}