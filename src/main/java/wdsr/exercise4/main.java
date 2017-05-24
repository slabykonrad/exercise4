package wdsr.exercise4;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class main {
	private static final Logger log = LoggerFactory.getLogger(main.class);
	private final static String connectionUri = "tcp://localhost:61616";
	private static Connection connectionToBroker;
	private static Session session;
	private static Destination destination;
	private static MessageProducer messageProducer;
	private static int first = 10000;
	
	public static void main(String[] args) {
		
		try {
			connectionToBroker = new ActiveMQConnectionFactory(connectionUri).createConnection();
			connectionToBroker.start();
			session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue("slabykonrad.QUEUE");
			messageProducer = session.createProducer(destination);
			
			TextMessage message = session.createTextMessage();
			message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
			
			long start_time = System.nanoTime();
			for(int i=0; i<first; ++i){
				message.setText("test_" + i);
				messageProducer.send(message);
			}
			long end_time = System.nanoTime();
			log.info("10000 persistent messages sent in {" + ((end_time - start_time)/1e6) + "} milliseconds.\n");
			
			message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
			start_time = System.nanoTime();
			for(int i=0; i<first; ++i){
				message.setText("test_" + i);
				messageProducer.send(message);
			}
			end_time = System.nanoTime();
			log.info("10000 non-persistent messages sent in {" + ((end_time - start_time)/1e6) + "} milliseconds.\n");
			
			
			
			
			messageProducer.close();
			session.close();
			connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
		
	}

}
