package com.wavefront.tools.wftop.components;

import java.util.Map;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 7/22/19
 *
 * @author Joanna Ko (kjoanna@vmware.com).
 */

public class NamespaceBuilderTest {
    private NamespaceBuilder testNamespaceBuilder = new NamespaceBuilder();

    @Before
    public void setUp() {
        testNamespaceBuilder.reset();
    }

    @Test
    public void testAccept_noSeparators(){
        String metricString = "noSepMetricsString";
        testNamespaceBuilder.accept(metricString, "hostname", metricString, 0, false);
        assertEquals(1, testNamespaceBuilder.getRoot().getNodes().size());
        NamespaceBuilder.Node tempNode = Iterables.getOnlyElement(testNamespaceBuilder.getRoot().getNodes().values());
        assertEquals(metricString, tempNode.getValue());
        assertEquals(0, testNamespaceBuilder.getRoot().getNodes().get(metricString).getNodes().size());
    }

    @Test
    public void testAccept_defaultSeparators(){
        String metricString = "Sep.Metrics.String";
        testNamespaceBuilder.accept(metricString, "hostname", metricString, 0, false);
        assertEquals(1, testNamespaceBuilder.getRoot().getNodes().size());
        assertTrue(testNamespaceBuilder.getRoot().getNodes().containsKey("Sep."));
        assertEquals(1, testNamespaceBuilder.getRoot().getNodes().get("Sep.").getNodes().size());
        assertTrue(testNamespaceBuilder.getRoot().getNodes().get("Sep.").getNodes().containsKey("Metrics."));
        assertEquals(1, testNamespaceBuilder.getRoot().getNodes().get("Sep.").getNodes().get("Metrics.").getNodes().size());
    }

    @Test
    public void testAccept_configSeparators(){
        String metricString = "Sep$Metrics^String";
        testNamespaceBuilder.setSeparatorCharacters("$%^");
        testNamespaceBuilder.accept(metricString, "hostname", metricString, 0, false);
        assertTrue(testNamespaceBuilder.getRoot().getNodes().containsKey("Sep$"));
        assertTrue(testNamespaceBuilder.getRoot().getNodes().get("Sep$").getNodes().containsKey(("Metrics^")));
    }

    @Test
    public void testAccept_rootCount() {
        String string1 = "sldb.fake";
        String string2 = "telegraf.fake";
        testNamespaceBuilder.accept(string1, "hostname", string1, 0, false);
        testNamespaceBuilder.accept(string2, "hostname", string2, 0, false);
        assertEquals(2, testNamespaceBuilder.getRoot().getNodes().size());
        assertTrue(testNamespaceBuilder.getRoot().getNodes().containsKey("sldb."));
        assertTrue(testNamespaceBuilder.getRoot().getNodes().containsKey("telegraf."));
    }

    @Test
    public void testAccept_childCount(){
        String rootStr = "sldb.";
        for (int i=0; i < 3; i++){
            testNamespaceBuilder.accept(rootStr + i, "hostname",rootStr + i, 0, false);
        }
        assertEquals(3, testNamespaceBuilder.getRoot().getNodes().get(rootStr).getNodes().size());
    }

    private boolean treeWithinChildLimit(NamespaceBuilder.Node root, int branchLimit){
        Map<String, NamespaceBuilder.Node> lookupTable = root.getNodes();
        if (root.getNodes().size() > branchLimit)
            return false;
        for (Map.Entry<String, NamespaceBuilder.Node> node: lookupTable.entrySet()) {
            if (root.getNodes().size() > branchLimit)  return false;
            treeWithinChildLimit(node.getValue(), branchLimit);
        }
        return true;
    }

    @Test
    public void testAccept_branchLimitReached(){
        String rootStr = "sldb.";
        testNamespaceBuilder.setMaxChildren(3);
        for (int i=0; i < 5; i++){
            testNamespaceBuilder.accept(rootStr + i, "hostname", rootStr + i, 0, false);
        }
        testNamespaceBuilder.accept("s3.fake", "hostname", "s3.fake", 0, false);
        testNamespaceBuilder.accept("telegraf.fake", "hostname", "telegraf.fake", 0, false);
        testNamespaceBuilder.accept("query.fake", "hostname", "query.fake", 0, false);
        assertTrue(treeWithinChildLimit(testNamespaceBuilder.getRoot(), testNamespaceBuilder.getMaxChildren()));
    }

    private int calcTreeDepth(NamespaceBuilder.Node root){
        int height = 0;
        Map<String, NamespaceBuilder.Node> lookupTable = root.getNodes();
        if (root.getNodes().size() < 1)
            return height;
        for (Map.Entry<String, NamespaceBuilder.Node> node: lookupTable.entrySet()){
            height = Math.max(height, calcTreeDepth(node.getValue()));
        }
        return height + 1;
    }

    @Test
    public void testAccept_depthLimitReached(){  //recursively search depth of the namespace tree
        String nodeString = "sldb.test.to.exceed.limit";
        testNamespaceBuilder.setMaxDepth(3);
        testNamespaceBuilder.accept(nodeString, "hostname", nodeString, 0, false);
        assertEquals(3, calcTreeDepth(testNamespaceBuilder.getRoot()));
    }

    @Test
    public void testGetFlattened(){
        String stringOne = "Flatten.String", stringTwo = "noFlat.string2", stringThree = "noFlat.string3";
        testNamespaceBuilder.accept(stringOne, "hostname", stringOne, 0, false);
        testNamespaceBuilder.accept(stringTwo, "hostname", stringTwo, 0, false);
        testNamespaceBuilder.accept(stringThree, "hostname", stringThree, 0, false);
        assertEquals(stringOne, testNamespaceBuilder.getRoot().getNodes().get("Flatten.").getFlattened());
        assertEquals("noFlat.", testNamespaceBuilder.getRoot().getNodes().get("noFlat.").getFlattened());
    }
}