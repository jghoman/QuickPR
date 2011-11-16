import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Iterator;

public class QuickPR extends
        Vertex<Text, DoubleWritable, DoubleWritable, DoubleWritable> {
    /** Configuration from Configurable */
    private Configuration conf;

    /** How many supersteps to run */
    public static String SUPERSTEP_COUNT = "quickpr.superstepCount";

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            setVertexValue(vertexValue);
        }

        if (getSuperstep() < getConf().getInt(SUPERSTEP_COUNT, -1)) {
            long edges = getNumOutEdges();
            sendMsgToAllEdges(new DoubleWritable(getVertexValue().get() / edges));
        } else {
            voteToHalt();
        }
    }

  @Override
  public Configuration getConf() {
      return conf;
  }

  @Override
  public void setConf(Configuration conf) {
      this.conf = conf;
  }

}