import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
	public static void main(String[] args)
	{
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		
//		builder.setSpout("array_spout", new ArraySpout());
		builder.setSpout("ActiveMQSpout", new ActiveMQSpout());
		builder.setBolt("SplitBolt", new UsainBolt()).shuffleGrouping("ActiveMQSpout");
		builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("myCluster", config, builder.createTopology());
	}
}
