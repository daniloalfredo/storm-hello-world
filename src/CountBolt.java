import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class CountBolt implements IRichBolt{
	ArrayList<String> palavras = new ArrayList<String>();
	ArrayList<Integer> counts = new ArrayList<Integer>();
	OutputCollector output;
	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("palavra");
		int index = this.palavras.indexOf(word);
		if (index >= 0)
		{
			int contadorAtual = this.counts.get(index) + 1;
			this.counts.set(index, contadorAtual);
			System.out.println(word + " - " + contadorAtual);
		}
		else
		{
			this.palavras.add(word);
			this.counts.add(1);
			System.out.println(word + " - " + 1);
		}
		this.output.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.output = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		
		return null;
	}
	
}
