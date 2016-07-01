/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.sorting;

import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.util.Order;
import org.junit.Test;

import java.util.List;

public class SortGraphCollectionTest extends GradoopFlinkTestBase {

  @Test
  public void testSortGraphCollection() {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
          "g1:g{a = 10}[" +
            "(v1)" +
          "]" +
          "g2:gr{a = 0}[" +
            "(v1)" +
          "]" +
          "g3:gra{a = 3}[" +
            "(v1)" +
          "]" +
          "g4:grap{a = 100}[" +
            "(v1)" +
          "]" +
          "g5:graph{a = -40}[" +
            "(v1)" +
          "]"
      );

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      loader.getGraphCollectionByVariables(
        "g1", "g2", "g3", "g4", "g5");

    byte[] zehn = new byte[4];

    Bytes.putInt(zehn, 0, 10);



    byte[] minuseins = new byte[4];

    Bytes.putInt(minuseins, 0, -1);

    System.out.println(Bytes.compareTo(zehn, minuseins));

    collection = collection.sortBy("a", Order.ASCENDING);
    try {
      List<GraphHeadPojo> headList = collection.getGraphHeads().collect();
      for (GraphHeadPojo head : headList) {
        System.out.println(head);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
