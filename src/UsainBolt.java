import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class UsainBolt implements IRichBolt{
//	Map<String, Integer> counts = new HashMap<String, Integer>();
	OutputCollector output;
	public static int ID;
	private int myId;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getStringByField("taxi");
		String[] palavras = line.split(" ");
		for (int i = 0; i < palavras.length; i++)
		{
			String palavra = palavras[i];
			System.out.println("Split Bolt - " + this.myId + " - " + palavra);
			this.output.emit(new Values(palavra));
		}
		this.output.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		output = arg2;
		myId = ++ID;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("palavra"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
