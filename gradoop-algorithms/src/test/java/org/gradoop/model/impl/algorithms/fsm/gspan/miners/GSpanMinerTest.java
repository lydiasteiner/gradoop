package org.gradoop.model.impl.algorithms.fsm.gspan.miners;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable.PredictableTransactionsGenerator;

import org.gradoop.io.impl.tlf.TLFDataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.GSpanGraphTransactionsEncoder;

import org.gradoop.model.impl.algorithms.fsm.gspan.encoders
  .GSpanTLFGraphEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class GSpanMinerTest extends GradoopFlinkTestBase {

  @Test
  public void testMinersSeparatelyYeast() throws Exception {

    String tlfFile =  GSpanMinerTest
      .class.getResource("/data/tlf").getFile() + "/yeast.tlf";

    TLFDataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(tlfFile, config);

    FSMConfig fsmConfig = new FSMConfig(0.3f, false);

    GSpanTLFGraphEncoder encoder = new GSpanTLFGraphEncoder(fsmConfig);

    DataSet<GSpanGraph> edges = encoder
      .encode(dataSource.getTLFGraphs());

    for (GSpanMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(getConfig().getExecutionEnvironment());

      DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

//      frequentSubgraphs.print();
//
//      System.out.println("-------------------------");

      assertEquals(112, frequentSubgraphs.count());
    }
  }

  @Test
  public void testMinersSeparatelyPredictable() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(10, 1, true, getConfig())
        .execute();

    float threshold = 0.2f;

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(threshold);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    DataSet<GSpanGraph> edges = encoder.encode(transactions);

    for (GSpanMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        transactions.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

      assertEquals(
        PredictableTransactionsGenerator.containedFrequentSubgraphs(threshold),
        frequentSubgraphs.count());
    }
  }

  private Collection<GSpanMiner> getTransactionalFSMiners() {
    Collection<GSpanMiner> miners = Lists.newArrayList();

    miners.add(new GSpanBulkIteration());
    miners.add(new GSpanFilterRefine());
    return miners;
  }

  @Test
  public void testMinersVersusPredictable() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(30, 1, true, getConfig())
        .execute();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

    GSpanGraphTransactionsEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GSpanGraphTransactionsEncoder<>(fsmConfig);

    DataSet<GSpanGraph> graphs = encoder.encode(transactions);

    GSpanMiner iMiner = new GSpanBulkIteration();
    iMiner.setExecutionEnvironment(
      transactions.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedDFSCode>> iResult =
      iMiner.mine(graphs, encoder.getMinFrequency(), fsmConfig);

    GSpanMiner fsMiner = new GSpanFilterRefine();
    DataSet<WithCount<CompressedDFSCode>> frResult =
      fsMiner.mine(graphs, encoder.getMinFrequency(), fsmConfig);

    collectAndAssertTrue(Equals
      .cross(Count.count(iResult),
        Count.count(iResult.join(frResult).where(0).equalTo(0)))
    );
  }

}