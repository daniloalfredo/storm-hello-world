import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ArraySpout implements IRichSpout {
	String[] names = {"Marcio,Erlich", "Thais,Albertins", "Rafaela,Ramos", "Elizabeth,Lopes", "Diogo,Kabbaz",
            "Paulo,Oliveira", "Henrique,Cunha", "Lailson,Santos", "Houston,Santos", "Leo,Sobel", 
            "Danilo,Souza", "Jonathan,Pereira", "Edvaldo,Acayaba"};
	
    int i = 0;
	SpoutOutputCollector out;
	int contadorGeral = 0;
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		String value = names[this.i++];
		if (this.i >= names.length)
		{
			this.i = 0;
		}
		this.out.emit(new Values(value), this.contadorGeral++);
		System.out.println("ARRAY SPOUT - " + value);
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector output) {
		out = output;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
