

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;

import oracle.AQ.AQException;
import oracle.AQ.AQQueueTable;
import oracle.AQ.AQQueueTableProperty;
import oracle.jms.AQjmsDestination;
import oracle.jms.AQjmsDestinationProperty;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;

public class OracleAQClient {

	static String userName = "GGATE";
	static String QUEUE = "GG_AQ_TEST_3";
	static String qTable = "GG_AQ_TEST_TBL_3";
	public static QueueConnection getConnection() {

		String hostname = "redhat";
		String oracle_sid = "orcl";
		int portno = 1522;

		String password = "GGATE";
		String driver = "thin";
		QueueConnectionFactory QFac = null;
		QueueConnection QCon = null;
		try {
			// get connection factory , not going through JNDI here
			QFac = AQjmsFactory.getQueueConnectionFactory(hostname, oracle_sid, portno, driver);
			// create connection
			QCon = QFac.createQueueConnection(userName, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return QCon;
	}

	static void createQueue(String userName, String qTable, String queue) {
	    try {
	      QueueConnection QCon = getConnection();
	      AQQueueTableProperty qt_prop = null;
	      AQQueueTable q_table = null;
	      AQjmsDestinationProperty dest_prop;
	      //AQQueue _queue = null;
	      Session session = QCon.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
	      try {
			qt_prop = new AQQueueTableProperty("SYS.AQ$_JMS_TEXT_MESSAGE");
		} catch (AQException e) {
			e.printStackTrace();
		}
	      q_table = ((AQjmsSession) session).createQueueTable(userName, queue, qt_prop);
	      System.out.println("Qtable created");
	      dest_prop = new AQjmsDestinationProperty();
	      // create a queue
	      AQjmsDestination destination = (AQjmsDestination) ((AQjmsSession) session).createQueue(q_table, queue, dest_prop);
	      System.out.println("Queue created");
	      // start the queue 
	      try{
	      	destination.start(session, true, true);
	      }catch(Exception e){
	    	  if(e.getMessage().contains(" ORA-24022")) {
	    		  System.out.println("waring message :\n" + e.getMessage());
	    		  //just the warning
	    		  //skip the exception
	    	  }
	      }
	      } catch (Exception e) {
	    	  e.printStackTrace();
	     }
	}

	public static void sendMessage(String user, String queueName, String message) {

		try {
			QueueConnection QCon = getConnection();
			Session session = QCon.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
			QCon.start();
			Queue queue = ((AQjmsSession) session).getQueue(user, queueName);
			MessageProducer producer = session.createProducer(queue);
			TextMessage tMsg = session.createTextMessage(message);

			// set properties to msg since axis2 needs this parameters to find
			// the operation
			tMsg.setStringProperty("SOAPAction", "getQuote");
			producer.send(tMsg);
			System.out.println("Sent message = " + tMsg.getText());

			session.close();
			producer.close();
			QCon.close();

		} catch (JMSException e) {
			e.printStackTrace();
			return;
		}
	}

	public static void browseMessage(String user, String queueName) {
		Queue queue;
		try {
			QueueConnection QCon = getConnection();
			Session session = QCon.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);

			QCon.start();
			queue = ((AQjmsSession) session).getQueue(user, queueName);
			QueueBrowser browser = session.createBrowser(queue);
			Enumeration<?> enu = browser.getEnumeration();
			List<String> list = new ArrayList<String>();
			while (enu.hasMoreElements()) {
				TextMessage message = (TextMessage) enu.nextElement();
				list.add(message.getText());
			}
			for (int i = 0; i < list.size(); i++) {
				System.out.println("Browsed msg " + list.get(i));
			}
			browser.close();
			session.close();
			QCon.close();

		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	public static void consumeMessage(String user, String queueName) {
		Queue queue;
		try {
			QueueConnection QCon = getConnection();
			Session session = QCon.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
			QCon.start();
			queue = ((AQjmsSession) session).getQueue(user, queueName);
			MessageConsumer consumer = session.createConsumer(queue);
			TextMessage msg = (TextMessage) consumer.receive();
			System.out.println("MESSAGE RECEIVED " + msg.getText());

			consumer.close();
			session.close();
			QCon.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws Exception {
		
		//createQueue(userName, qTable, QUEUE);
		//sendMessage(userName, QUEUE, "<user>text</user>");
		//browseMessage(userName, QUEUE);
		consumeMessage(userName, QUEUE);
	}
}