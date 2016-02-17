import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

public class DBUtilsTest {

	public static void main(String[] args) throws SQLException {
		test_insert();
		test_find();
	}

	public static void test_insert() throws SQLException {
		System.out.println("-------------test_insert()-------------");
		// 创建连接
		Connection conn = ConnTools.makeConnection();
		// 创建SQL执行工具
		QueryRunner qRunner = new QueryRunner();
		// 执行SQL插入
		int n = qRunner.update(conn, "insert into user(name,pswd) values('iii','iii')");
		System.out.println("成功插入" + n + "条数据！");
		// 关闭数据库连接
		DbUtils.closeQuietly(conn);
	}

	public static void test_find() throws SQLException {
		System.out.println("-------------test_find()-------------");
		// 创建连接
		Connection conn = ConnTools.makeConnection();
		// 创建SQL执行工具
		QueryRunner qRunner = new QueryRunner();
		// 执行SQL查询，并获取结果
		// List<User> list = (List<User>) qRunner.query(conn, "select
		// id,name,pswd from user",
		// new BeanListHandler(User.class));
		// // 输出查询结果
		// for (User user : list) {
		// System.out.println(user);
		// }
		// 关闭数据库连接
		DbUtils.closeQuietly(conn);
	}

	static class ConnTools {
		private static final String dirverClassName = "com.mysql.jdbc.Driver";
		private static final String url = "jdbc:mysql://192.168.104.101:3306/testdb?useUnicode=true&characterEncoding=utf8";
		private static final String user = "root";
		private static final String password = "leizhimin";

		public static Connection makeConnection() {
			Connection conn = null;
			try {
				Class.forName(dirverClassName);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			try {
				conn = DriverManager.getConnection(url, user, password);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return conn;
		}
	}
}
