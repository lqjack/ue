
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class testDB {
	public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
	// public static final String DBURL =
	// "jdbc:oracle:thin:@127.0.0.1:1521/testdb1"; //sid 格式 testdb1是sid
	private static final String DBURL = "jdbc:oracle:thin:@redhat:1522:orcl"; // servicename
																				// TDB是service_name
	private static final String DBUSER = "sys as msysdba";
	private static final String DBPASSWORD = "Lqjacklee1";

	private String SRC_TABLE_NAME = "test";
	private String LOG_TABLE_NAME = "log";

	private static Connection conn;

	private void insertData() throws SQLException {
		assert (conn != null);
		String sql = " insert into " + SRC_TABLE_NAME + "(id,name) values(1,'a')";
		conn.createStatement().execute(sql);
		conn.commit();
	}

	private void updateData() throws SQLException {
		String sql = "update " + SRC_TABLE_NAME + " set name = 'b' where id = 1";
		conn.createStatement().executeUpdate(sql);
		conn.commit();
	}

	private void deleteData(boolean init) throws SQLException {
		String sql = "delete from " + SRC_TABLE_NAME;
		if (!init) {
			sql = sql + " where id = 1";
		}
		conn.createStatement().execute(sql);
		conn.commit();
	}

	private void selectData(boolean data) throws SQLException {
		String sql = "select * from ";
		if (data) {
			System.out.println(" data below : ");
			sql = sql + SRC_TABLE_NAME;
		} else {
			System.out.println(" log below : ");
			sql = sql + LOG_TABLE_NAME;
		}
		ResultSet rs = conn.createStatement().executeQuery(sql);
		ResultSetMetaData md = rs.getMetaData();
		while (rs.next()) {
			for (int i = 0; i < md.getColumnCount(); i++) {
				String colName = md.getColumnLabel(i + 1);
				int sqlType = md.getColumnType(i + 1);
				Class<?> type = JDBCTypesUtils.jdbcTypeToJavaType(sqlType);
				System.out.println("type:" + type);
				Object obj = rs.getObject(colName);
				obj = DBUtils.getValueBySqlType(rs,type,colName);
				System.out.println("colNama: " + colName + " value :" + obj);
			}
		}
	}

	public void test() throws SQLException {
		try {
			conn = getConnection();
			preData();
			insertData();
			updateData();
//			deleteData(false);

			showData();
			showLog();
		} finally {
			conn.close();
		}
	}

	private void showLog() throws SQLException {
		selectData(false);
	}

	private void showData() throws SQLException {
		selectData(true);
	}

	private void preData() throws SQLException {
		deleteData(true);
	}

	private static Connection getConnection() throws SQLException {
		String userName = "GGATE";
		String password = "GGATE";
		return DriverManager.getConnection(DBURL, userName, password);
	}

	// public static final String DBURL = "jdbc:oracle:thin:@ORCL"; // tnsname
	// 格式

	private void testDb() {
		try {
			Connection con = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			String strSQL = "select count(*) from test";
			// System.setProperty("oracle.net.tns_admin",
			// "$ORACLE_HOME/admin/oracle");// 使用tnsname
			Class.forName(DBDRIVER).newInstance();
			conn = getConnection();
			ps = con.prepareStatement(strSQL);
			rs = ps.executeQuery();
			while (rs.next()) {
				System.out.println("num:" + rs.getString(1));
			}
			rs.close();
			ps.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		testDB db = new testDB();
		db.test();
	}
}