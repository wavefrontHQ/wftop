package com.wavefront.tools.wftop.components;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test for when spying by Id Creation.
 * Spy Type is determined by typePrefix when building spy url in {@link PointsSpy}.
 *
 * @author Joanna Ko (kjoanna@vmware.com)
 */
public class IdCreationTest {
  private RootNode root = new RootNode("root");
  public final String keyString = "newIdKey=";

  @Before
  public void setUp() {
    root.reset();
  }

  /**
   * Tests NamespaceBuilder when Ids are added to root node.
   */
  @Test
  public void addIdCreation() {
    addPoints();

    //one source node ("None")
    assertEquals(1, root.getNodes().size());
    assertTrue(root.getNodes().containsKey("None"));

    SourceNode sourceNode = root.getNodes().get("None");
    assertTrue(sourceNode.getNodes().containsKey(keyString));
    assertEquals(1, sourceNode.getNodes().size());

    //check now separate by = (default separators now -, ., _, =)
    NamespaceBuilder namespaceBuilder = sourceNode.getNamespaceBuilder();
    assertTrue(namespaceBuilder.getRoot().getNodes().containsKey(keyString));

    Map<String, NamespaceNode> NamespaceNodes = namespaceBuilder.getRoot().getNodes().get(keyString).getNodes();
    assertEquals(5, NamespaceNodes.size());

    //Id Points have no access value
    NamespaceNodes.forEach((k, v) -> assertEquals(0, v.getAccessed()));

  }

  /**
   * Given type Prefix and Sample rate, tests Spy Url is built correctly when spying on Id Creations.
   */
  @Test
  public void testSpyUrl() {
    //Set Up Spying
    String fakeClusterUrl = "test.wavefront.com", fakeToken = "abcde-12345";
    String spyUrlBeginning = "https://" + fakeClusterUrl + "/api/spy/ids?";
    PointsSpy pointsSpy = new PointsSpy();
    pointsSpy.setSpyOn(false);

    // Given no type
    pointsSpy.setParameters(fakeClusterUrl, fakeToken, null, null, 1.0);
    assertEquals(
        spyUrlBeginning + "type=&name=&includeScalingFactor=true&sampling=1.0",
        pointsSpy.getSpyUrl());

    pointsSpy.setParameters(fakeClusterUrl, fakeToken, "METRIC", null, 1.0);
    assertEquals(
        spyUrlBeginning + "type=METRIC&name=&includeScalingFactor=true&sampling=1.0",
        pointsSpy.getSpyUrl());

    pointsSpy.setParameters(fakeClusterUrl, fakeToken, "HOST", null, 0.1);
    assertEquals(
        spyUrlBeginning + "type=HOST&name=&includeScalingFactor=true&sampling=0.1",
        pointsSpy.getSpyUrl());

    //In PointSpy, POINT_TAG is changed to STRING
    pointsSpy.setParameters(fakeClusterUrl, fakeToken, "STRING", null, 0.5);
    assertEquals(
        spyUrlBeginning + "type=STRING&name=&includeScalingFactor=true&sampling=0.5",
        pointsSpy.getSpyUrl());

    pointsSpy.setParameters(fakeClusterUrl, fakeToken, "HISTOGRAM", null, 0.75);
    assertEquals(
        spyUrlBeginning + "type=HISTOGRAM&name=&includeScalingFactor=true&sampling=0.75",
        pointsSpy.getSpyUrl());

    pointsSpy.setParameters(fakeClusterUrl, fakeToken, "SPAN", null, 1.0);
    assertEquals(
        spyUrlBeginning + "type=SPAN&name=&includeScalingFactor=true&sampling=1.0",
        pointsSpy.getSpyUrl());
  }

  public void addPoints() {
    for (int i = 0; i < 5; i++) {
      root.accept(keyString + "newIdValue" + i);
    }
  }
}
