package wdsr.exercise4.receiver;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;
import wdsr.exercise4.sender.JmsSender;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private final String connectionUri = "tcp://localhost:61616";
	private Connection connectionToBroker;
	private Session session;
	private Destination destination;
	private MessageConsumer messageConsumer;
	private AlertService alertService;
	
	static final String PRICE_ALERT = "PriceAlert";
	static final String VOLUME_ALERT = "VolumeAlert";
	
	 
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(this.connectionUri);
		
		try {
			factory.setTrustAllPackages(true);
			this.connectionToBroker = factory.createConnection();
			this.connectionToBroker.start();
			this.session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.destination = session.createQueue(queueName);
			this.messageConsumer = session.createConsumer(this.destination);
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		try {
			this.messageConsumer.setMessageListener(new MessageListener() {
				
				@Override
				public void onMessage(Message message) {
					if(message instanceof TextMessage) {
						TextMessage text = (TextMessage) message;
						try {
							String messageType = message.getJMSType().toString();
							if (messageType.equals(PRICE_ALERT)){
								ArrayList<String> price = splitMessage(text.getText());
								PriceAlert priceAlert = new PriceAlert(new Long(price.get(0)), price.get(1), new BigDecimal(price.get(2).trim()));
								alertService.processPriceAlert(priceAlert);
							}
							else if(messageType.equals(VOLUME_ALERT)){
								ArrayList<String> voulme = splitMessage(text.getText());
								VolumeAlert volumeAlert = new VolumeAlert(new Long(voulme.get(0)), voulme.get(1), new Long(voulme.get(2).trim()));
								alertService.processVolumeAlert(volumeAlert);
							}
						} catch (JMSException e) {
							log.error(e.getMessage());
						}
					} 
					else if(message instanceof ObjectMessage) {
						ObjectMessage object = (ObjectMessage) message;
						try {
							String messageType = message.getJMSType().toString();
							if (messageType.equals(PRICE_ALERT)){
								PriceAlert priceAlert = (PriceAlert) object.getObject();
								alertService.processPriceAlert(priceAlert);
							}
							else if(messageType.equals(VOLUME_ALERT)){
								VolumeAlert volumeAlert = (VolumeAlert) object.getObject();
								alertService.processVolumeAlert(volumeAlert);
							}
						} catch (JMSException e) {
							log.error(e.getMessage());
						}
					} 
					else {
						log.info("Wrong message");
					}
				}
			});
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		
		try {
			messageConsumer.close();
			session.close();
			connectionToBroker.close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}
	
	private ArrayList<String> splitMessage(String message) {
		String[] split = message.split("\n");
		ArrayList<String> values = new ArrayList<>();
		for(String s: split) {
			String[] tmp = s.split("=");
		 	values.add(tmp[1]);
		}
		return values;
	}
	
	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.   
}
