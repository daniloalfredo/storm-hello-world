import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ActiveMQSpout implements IRichSpout, MessageListener{

	private LinkedBlockingQueue<Message> queue;
	private MessageConsumer msgConsumer;
	SpoutOutputCollector collector;
	
	public void onMessage(Message m)
	{
		this.queue.offer(m);
	}

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
		Message msg = this.queue.poll();
		if (msg == null)
		{
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			if (msg instanceof TextMessage)
			{
				try {
					String taxi = ((TextMessage) msg).getText();
					System.out.println("MQ Spout - " + taxi);
					this.collector.emit(new Values(taxi));
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
			else
			{
				this.collector.emit(null);
			}
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector output) {
		this.collector = output;
		ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		try
		{
			Connection conn = connFactory.createConnection();
			Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination dest = session.createQueue("XEPA");
			this.msgConsumer = session.createConsumer(dest);
			this.queue = new LinkedBlockingQueue<Message>();
			this.msgConsumer.setMessageListener(this);
			conn.start();
		} catch (JMSException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("taxi"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
