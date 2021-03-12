import org.apache.activemq.*;

import java.io.File;
import java.util.Scanner;
import java.util.StringTokenizer;

import javax.jms.*;

public class Produtor {
	public static void main(String[] args)
	{	
		try
		{
			//Create Connection Factory
			ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			
			//Create a connection
			Connection conn = connFactory.createConnection();
			conn.start();
			
			//Create session
			Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			//Create the destination (Topic or Queue)
			Destination dest = session.createQueue("XEPA");
			
			//Create Messageproducer from the Session to the Topic or Queue
			MessageProducer producer = session.createProducer(dest);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			
			//Read file
			File file = new File("/home/dams/Documents/POS/taxi.txt");
			Scanner s = new Scanner(file);
			TextMessage message;
			
			while(s.hasNext())
			{
				String line = s.next();
				StringTokenizer st = new StringTokenizer(line, ",");
				message = session.createTextMessage(line);
//				message.setIntProperty("placa", Integer.parseInt(st.nextToken()));
				producer.send(message);
			}
			
			s.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
