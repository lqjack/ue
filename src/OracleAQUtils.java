
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.aq.AQAgent;
import oracle.jdbc.aq.AQDequeueOptions;
import oracle.jdbc.aq.AQEnqueueOptions;
import oracle.jdbc.aq.AQFactory;
import oracle.jdbc.aq.AQMessage;
import oracle.jdbc.aq.AQMessageProperties;
import oracle.jdbc.aq.AQNotificationEvent;
import oracle.jdbc.aq.AQNotificationListener;
import oracle.jdbc.aq.AQNotificationRegistration;

public class OracleAQUtils {

	static final String USERNAME = "GGATE";
	static final String PASSWORD = "GGATE";
	static final String URL = "jdbc:oracle:thin:@redhat:1522:orcl";
	private static final String QUEUE_INNER_TABLE_PRE = "queue.inner.table.pre";

	private static final String RAW_MUTIPLY_QUEUE = ".RAW_MUTIPLY_QUEUE";
	private static final String RAW_SINGLE_QUEUE = ".RAW_SINGLE_QUEUE";
	private static final String RAW_QUEUE_TYPE = "RAW";

	private String queueName;
	private OracleConnection connection;
	private AQRawQueueListener queueListener;
	private AQNotificationRegistration notificationReg;
	private String INNER_EXCEPTION_QUEUE = "inner.exception.queue";
	private String INNER_CORRELATION = "inner.correlation";
	private String INNER_AQ_AGENT_NAME = "inner.aq.agent.name";
	private String INNER_AQ_AGENT_ADDRESS = "inner.aq.agent.address";
	private AQAgent[] recipients;

	private static Logger log = LoggerFactory.getLogger(OracleAQUtils.class);

	public OracleConnection connect(String userName, String password, String url) throws SQLException {
		OracleDriver dr = new OracleDriver();
		Properties prop = new Properties();
		prop.setProperty("user", userName);
		prop.setProperty("password", password);
		log.info("user :" + userName + " is connecting the url : " + url);
		this.connection = (OracleConnection) dr.connect(url, prop);
		return (OracleConnection) dr.connect(url, prop);
	}

	public void initAQ(boolean bCleanTable, boolean bSetup) throws SQLException {
		assert (connection != null);
		connection.setAutoCommit(false);// TODO:哪里结束
		log.info(" conncetion init");
		if (bCleanTable) {
			cleanup(QUEUE_INNER_TABLE_PRE, true);
			log.info("clean aq table finish");
		}
		if (bSetup) {
			setup(QUEUE_INNER_TABLE_PRE, true);
			log.info("setup aq table finish");
		}
	}

	public void closeConnection() {
		try {
			assert (connection != null);
			connection.close();
			log.info("close connection " + connection.toString() + "finish");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (!connection.isClosed()) {
					connection.clearWarnings();
					connection.close();
					connection = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private String getQueueName(String queueName, boolean bSingleQueue) {
		String queuePre = (queueName != null && !"".equals(queueName)) ? queueName : QUEUE_INNER_TABLE_PRE;
		String queueSuffix = bSingleQueue ? RAW_SINGLE_QUEUE : RAW_MUTIPLY_QUEUE;
		queueName = queuePre + queueSuffix;
		log.info("queue Name :" + queueName);
		return queueName;
	}

	public void registeNotification(String queueName, AQAgent[] recipients) {
		AQNotificationRegistration notificationReg = registerForAQEvents();
		this.notificationReg = notificationReg;
		AQRawQueueListener singleListener = new AQRawQueueListener(queueName, RAW_QUEUE_TYPE);
		this.queueListener = singleListener;
		this.recipients = recipients;
		try {
			notificationReg.addListener(singleListener);
		} catch (SQLException e1) {
			log.error("registe the queue listener failed");
			e1.printStackTrace();
		}
		while (singleListener.getEventsCount() < 1) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void unregisteNotification() {
		try {
			queueListener.closeConnection();
			connection.unregisterAQNotification(notificationReg);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void enqueueMessage(String queueName, String message) throws SQLException {
		enqueueMessage(queueName, null, message, null);
	}

	public void enqueueMessage(String queueName, String message, AQAgent sender) throws SQLException {
		enqueueMessage(queueName, null, message, sender);
	}

	public void enqueueMessage(String queueName, AQAgent[] recipients, String message, AQAgent sender)
			throws SQLException {
		log.info("----------- Enqueue start ------------");
		long beginTime = System.currentTimeMillis();
		// First create the message properties:
		AQMessageProperties msgprop = AQFactory.createAQMessageProperties();
		msgprop.setCorrelation(INNER_CORRELATION);
		msgprop.setExceptionQueue(INNER_EXCEPTION_QUEUE);

		// Specify an agent as the sender:
		if (sender != null) {
			msgprop.setSender(sender);
		} else {
			AQAgent ag = AQFactory.createAQAgent();
			ag.setName(INNER_AQ_AGENT_NAME);
			ag.setAddress(INNER_AQ_AGENT_ADDRESS);
			msgprop.setSender(ag);
		}

		// handle multi consumer case:
		if (recipients != null)
			msgprop.setRecipientList(recipients);
		else
			msgprop.setRecipientList(recipients);
		log.info(msgprop.toString());
		boolean bSingle = true;
		if (msgprop.getRecipientList() != null)
			bSingle = false;

		// Create the actual AQMessage instance:
		AQMessage mesg = AQFactory.createAQMessage(msgprop);
		// and add a payload:
		assert (message != null);
		assert (!"".equals(message));
		oracle.sql.RAW raw = new oracle.sql.RAW(message.getBytes());
		mesg.setPayload(raw);
		// We want to retrieve the message id after enqueue:
		AQEnqueueOptions opt = new AQEnqueueOptions();
		opt.setRetrieveMessageId(true);

		// execute the actual enqueue operation:
		connection.enqueue(getQueueName(queueName, bSingle), opt, mesg);

		byte[] mesgId = mesg.getMessageId();

		if (mesgId != null)
			log.info("Message ID from enqueue call: " + byteBufferToHexString(mesgId, 20));
		log.info("----------- Enqueue done ------------ ,using :" + (System.currentTimeMillis() - beginTime) / 1000
				+ "s");
	}

	public String dequeue(String queueName) throws SQLException {
		return dequeue(queueName, null);
	}

	public String dequeue(String queueName, String consumerName) throws SQLException {
		return dequeue(queueName, RAW_QUEUE_TYPE, consumerName);
	}

	public String dequeue(String queueName, String queueType, String consumerName) throws SQLException {
		log.info("----------- Dequeue start ------------");
		long beginTime = getCurrentTime();
		AQDequeueOptions deqopt = new AQDequeueOptions();
		deqopt.setRetrieveMessageId(true);
		if (consumerName != null)
			deqopt.setConsumerName(consumerName);
		boolean bSingle = recipients == null;
		queueName = getQueueName(queueName, bSingle);

		// dequeue operation:
		AQMessage msg = connection.dequeue(queueName, deqopt, queueType);

		// print out the message that has been dequeued:
		byte[] payload = msg.getPayload();
		byte[] msgId = msg.getMessageId();
		if (msgId != null) {
			String mesgIdStr = byteBufferToHexString(msgId, 20);
			log.info("ID of message dequeued = " + mesgIdStr);
		}
		AQMessageProperties msgProp = msg.getMessageProperties();
		log.info("message properties :" + msgProp.toString());
		String payloadStr = new String(payload, 0, payload.length);
		log.info("payload.length=" + payload.length + ", value=" + payloadStr);
		log.info("----------- Dequeue done ------------ ,using " + (getCurrentTime() - beginTime) / 1000 + "s");
		return payloadStr;
	}

	public AQNotificationRegistration registerForAQEvents() {
		assert (connection != null);
		assert (queueName != null && !"".equals(queueName));
		Properties globalOptions = new Properties();
		String[] queueNameArr = new String[1];
		queueNameArr[0] = queueName;
		Properties[] opt = new Properties[1];
		opt[0] = new Properties();
		opt[0].setProperty(OracleConnection.NTF_AQ_PAYLOAD, "true");
		AQNotificationRegistration[] regArr = null;
		try {
			regArr = connection.registerAQNotification(queueNameArr, opt, globalOptions);
		} catch (SQLException e) {
			log.error("connection :" + connection + " registe notification failed");
			e.printStackTrace();
		}
		AQNotificationRegistration reg = regArr[0];
		return reg;
	}

	private void setup(String tablePre, boolean bSingleQueue) throws SQLException {
		long beginTime = getCurrentTime();
		tablePre = tablePre != null ? tablePre : QUEUE_INNER_TABLE_PRE;

		StringBuffer createQueueTableBuffer = new StringBuffer(
				"BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(  QUEUE_TABLE        =>  '");
		createQueueTableBuffer.append(tablePre + "");
		if (bSingleQueue)
			createQueueTableBuffer.append(RAW_SINGLE_QUEUE);
		else
			createQueueTableBuffer.append(RAW_MUTIPLY_QUEUE);
		createQueueTableBuffer.append("',  QUEUE_PAYLOAD_TYPE =>  '" + RAW_QUEUE_TYPE + "', ");
		if (!bSingleQueue)
			createQueueTableBuffer.append("   MULTIPLE_CONSUMERS =>  TRUE, ");
		createQueueTableBuffer.append("   COMPATIBLE         =>  '10.0'); END; ");
		StringBuffer createQueueBuffer = new StringBuffer(
				"BEGIN DBMS_AQADM.CREATE_QUEUE(   QUEUE_NAME     =>   '" + tablePre + "");
		if (bSingleQueue)
			createQueueBuffer.append(RAW_SINGLE_QUEUE);
		else
			createQueueBuffer.append(RAW_MUTIPLY_QUEUE);
		createQueueBuffer.append("',   QUEUE_TABLE    =>   '" + tablePre + "");
		if (bSingleQueue)
			createQueueBuffer.append(RAW_SINGLE_QUEUE);
		else
			createQueueBuffer.append(RAW_MUTIPLY_QUEUE);
		createQueueBuffer.append("'); END;  ");
		StringBuffer startQueueBuffer = new StringBuffer("BEGIN  DBMS_AQADM.START_QUEUE('");
		startQueueBuffer.append(tablePre);
		if (bSingleQueue)
			startQueueBuffer.append(RAW_SINGLE_QUEUE);
		else
			startQueueBuffer.append(RAW_MUTIPLY_QUEUE);
		startQueueBuffer.append("'); END; ");
		doUpdateDatabase(createQueueTableBuffer.toString());
		doUpdateDatabase(createQueueBuffer.toString());
		doUpdateDatabase(startQueueBuffer.toString());
		log.info("set up aq using " + (getCurrentTime() - beginTime) / 1000 + "s");
	}

	void cleanup(String tablePre, boolean bSingleQueue) {
		long beginTime = getCurrentTime();
		tablePre = tablePre != null ? tablePre : QUEUE_INNER_TABLE_PRE;

		StringBuffer stopQueueBuffer = new StringBuffer("BEGIN  DBMS_AQADM.STOP_QUEUE('" + tablePre + "");
		if (bSingleQueue)
			stopQueueBuffer.append(RAW_SINGLE_QUEUE);
		else
			stopQueueBuffer.append(RAW_MUTIPLY_QUEUE + "    FORCE               => TRUE");
		stopQueueBuffer.append("'); END; ");
		StringBuffer dropQueueBuffer = new StringBuffer(
				"BEGIN  DBMS_AQADM.DROP_QUEUE_TABLE( " + "    QUEUE_TABLE     => '" + tablePre);
		if (bSingleQueue)
			dropQueueBuffer.append(RAW_SINGLE_QUEUE);
		else
			dropQueueBuffer.append(RAW_MUTIPLY_QUEUE + "', " + "    FORCE   => TRUE");
		dropQueueBuffer.append("); END; ");
		updateDatabase(stopQueueBuffer.toString());
		updateDatabase(dropQueueBuffer.toString());
		log.info("clean up aq using " + (getCurrentTime() - beginTime) / 1000 + "s");
	}

	private void updateDatabase(String sql) {
		long beginTime = getCurrentTime();
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException sqlex) {
			log.info("Exception (" + sqlex.getMessage() + ") while trying to execute \"" + sql + "\",using "
					+ (getCurrentTime() - beginTime) / 1000 + "");
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException exx) {
					exx.printStackTrace();
				}
			}
		}
	}

	private void doUpdateDatabase(String sql) throws SQLException {
		long beginTime = getCurrentTime();
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.executeUpdate(sql);
			log.info("execute the sql '" + sql + "',using " + (getCurrentTime() - beginTime) / 1000 + "s");
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException exx) {
				}
			}
		}
	}

	private long getCurrentTime() {
		return System.currentTimeMillis();
	}

	static final String byteBufferToHexString(byte[] buffer, int maxNbOfBytes) {
		if (buffer == null)
			return null;
		int offset = 0;
		boolean isFirst = true;
		StringBuffer sb = new StringBuffer();
		while (offset < buffer.length && offset < maxNbOfBytes) {
			if (!isFirst)
				sb.append(' ');
			else
				isFirst = false;
			String hexrep = Integer.toHexString((int) buffer[offset] & 0xFF);
			if (hexrep.length() == 1)
				hexrep = "0" + hexrep;
			sb.append(hexrep);
			offset++;
		}
		return sb.toString();
	}
}

class AQRawQueueListener implements AQNotificationListener {
	private static final Logger log = LoggerFactory.getLogger(AQRawQueueListener.class);
	OracleConnection conn;
	String queueName;
	String typeName;
	int eventsCount = 0;

	public AQRawQueueListener(String _queueName, String _typeName) {
		queueName = _queueName;
		typeName = _typeName;
		try {
			conn = (OracleConnection) DriverManager.getConnection(OracleAQUtils.URL, OracleAQUtils.USERNAME,
					OracleAQUtils.PASSWORD);
		} catch (SQLException e) {
			log.error("");
			e.printStackTrace();
		}
	}

	public void onAQNotification(AQNotificationEvent e) {
		log.info("\n----------- DemoAQRawQueueListener: got an event ----------- ");
		log.info(e.toString());
		log.info("------------------------------------------------------");

		log.info("----------- DemoAQRawQueueListener: Dequeue start -----------");
		try {
			AQDequeueOptions deqopt = new AQDequeueOptions();
			deqopt.setRetrieveMessageId(true);
			if (e.getConsumerName() != null)
				deqopt.setConsumerName(e.getConsumerName());
			// if ((e.getMessageProperties()).getDeliveryMode() ==
			// AQMessageProperties.MESSAGE_BUFFERED) {
			// deqopt.setDeliveryMode(AQDequeueOptions.DEQUEUE_BUFFERED);
			// deqopt.setVisibility(AQDequeueOptions.DEQUEUE_IMMEDIATE);
			// }
			AQMessage msg = conn.dequeue(queueName, deqopt, typeName);
			byte[] msgId = msg.getMessageId();
			if (msgId != null) {
				String mesgIdStr = OracleAQUtils.byteBufferToHexString(msgId, 20);
				log.info("ID of message dequeued = " + mesgIdStr);
			}
			log.info(msg.getMessageProperties().toString());
			byte[] payload = msg.getPayload();
			if (typeName.equals("RAW")) {
				String payloadStr = new String(payload, 0, 10);
				log.info("payload.length=" + payload.length + ", value=" + payloadStr);
			}
			log.info("----------- DemoAQRawQueueListener: Dequeue done -----------");
		} catch (SQLException sqlex) {
			log.info(sqlex.getMessage());
		}
		eventsCount++;
	}

	public int getEventsCount() {
		return eventsCount;
	}

	public void closeConnection() throws SQLException {
		conn.close();
	}
}
