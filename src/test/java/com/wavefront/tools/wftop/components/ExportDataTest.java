package com.wavefront.tools.wftop.components;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.wavefront.tools.wftop.panels.IdNamespacePanel;
import com.wavefront.tools.wftop.panels.NamespacePanel;
import com.wavefront.tools.wftop.panels.PointsNamespacePanel;
import org.junit.*;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportDataTest {
  private static NamespacePanel namespacePanel;
  private final String fileName = "exportDataTest.csv";
  private RootNode root = new RootNode("root");
  private BufferedReader csvReader;

  /**
   * Test if CSV file is created in current directory.
   */
  @Test
  public void testCSVCreated() {
    namespacePanel = new PointsNamespacePanel(null, null);
    namespacePanel.setExportData(true, fileName);
    String currentDir = "./";
    File file = new File(currentDir, fileName);
    assertTrue(file.exists());
  }

  /**
   * Checks root path is set accordingly. Root path is the path displayed when exporting data.
   */
  @Test
  public void testSetRootPath() {
    namespacePanel.setRootPath("METRIC: ");
    assertEquals("METRIC:", namespacePanel.getRootPath());

    namespacePanel.setRootPath("METRIC: this.path");
    assertEquals("this.path", namespacePanel.getRootPath());
  }

  /**
   * Test Points are written correctly to CSV file.
   */
  @Test
  public void testWritePointCSV() {
    //add points to PointsNamespacePanel
    Multimap<String, String> noSource =
        ImmutableMultimap.<String, String>builder().put("pointTagKey", "pointTagValue").build();
    for (int i = 1; i <= 5; i++) {
      root.accept(Dimension.METRIC, false, false,
          "metric" + i, "host" + i, noSource, 0, 0);
    }
    //Write to CSV file
    assertTrue(root.getNodes().containsKey("None"));
    Node sourceNode = root.getNodes().get("None");
    assertEquals(5, root.getNodes().get("None").getNodes().size());

    namespacePanel.setRootPath("METRIC: ");
    namespacePanel.setExportData(true, fileName);
    namespacePanel.renderNodes(sourceNode, 0.01, sourceNode.getNodes().values(), true);

    //Check CSV heading output
    try {
      csvReader = new BufferedReader(new FileReader("./" + fileName));
      String row = csvReader.readLine();
      assertTrue(hasCorrectPointHeader(row.split(",")));
      row = csvReader.readLine();
      String [] rowItems = row.split(",");
      assertEquals("METRIC:", rowItems[0]);
      for (int i = 0; i <= 4; i++) {
        row = csvReader.readLine();
        rowItems = row.split(",");
        assertEquals("metric" + (i+1), rowItems[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private boolean hasCorrectPointHeader(String [] header) {
    return (header[0].equals("Namespace") && header[1].equals("PPS") &&
        header[2].equals("% Acc.") && header[3].equals("P50 Lag") &&
        header[4].equals("P75 Lag") && header[5].equals("P99 Lag") &&
        header[6].equals("Num Metrics") && header[7].equals("Num Hosts") &&
        header[8].equals("Range"));
  }

  /**
   * Test Ids are written correctly to CSV file.
   */
  @Test
  public void testWriteIdCSV() {
    namespacePanel = new IdNamespacePanel(null, null);
    //add points to IdNamespacePanel
    for (int i = 0; i < 5; i++) {
      root.accept("newIdKey" + i + "=newIdValue");
    }
    //Write to CSV file
    assertTrue(root.getNodes().containsKey("None"));
    Node sourceNode = root.getNodes().get("None");
    assertEquals(5, sourceNode.getNodes().size());

    namespacePanel.setRootPath("POINT_TAG: ");
    namespacePanel.setExportData(true, fileName);
    namespacePanel.renderNodes(sourceNode, 0.01, sourceNode.getNodes().values(), true);

    //Check CSV Id header output
    try {
      csvReader = new BufferedReader(new FileReader( "./" + fileName));
      String row = csvReader.readLine();
      assertTrue(hasCorrectIdHeader(row.split(",")));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private boolean hasCorrectIdHeader(String [] header) {
    return (header[0].equals("Namespace") && header[1].equals("CPS") &&
        header[2].equals("Num Metrics"));
  }

  @After
  public void cleanUp() {
    File file = new File("./" + fileName);
    file.delete();
  }
}
