import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FileSpout implements IRichSpout {
	Scanner s = null;
	SpoutOutputCollector out;
	int contador = 0;
	@Override
	public void ack(Object arg0) {
		
		
	}
	@Override
	public void activate() {
		
		
	}
	@Override
	public void close() {
		
		
	}
	@Override
	public void deactivate() {
		
		
	}
	@Override
	public void fail(Object arg0) {
		
		
	}
	@Override
	public void nextTuple() {
		String taxiReg = this.s.nextLine();
		out.emit(new Values(taxiReg), this.contador++);
		System.err.println("Arquivo Spout - " + taxiReg);
	}
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector output) {
		this.out = output;
		File f = new File("/home/dams/Documents/POS/taxi.txt");
		try {
			s = new Scanner(f);
		} 
		catch (FileNotFoundException e) {}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("linha"));
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		
		return null;
	}
}
