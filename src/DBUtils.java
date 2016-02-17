import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * 
 * @author liu
 * 
 *         oracel数据类型跟java类型的对应 rid:java.lang.String
 *         type_binary_double:java.lang.Double type_binary_float:java.lang.Float
 *         type_blob:oracle.sql.BLOB type_clob:oracle.sql.CLOB
 *         type_date:java.sql.Timestamp type_long:java.lang.String
 *         type_number:java.math.BigDecimal type_nvarchar2:java.lang.String
 *         type_raw:[B type_timestamp:oracle.sql.TIMESTAMP
 *         type_timestamp_with_local:oracle.sql.TIMESTAMPLTZ
 *         type_timestamp_with_time:oracle.sql.TIMESTAMPTZ
 *         type_varchar2:java.lang.String
 *
 * @param <T>
 */
public class DBUtils<T> {

	public T handler(ResultSet rs, Class<T> clazz) {
		T entity = null;
		try {
			ResultSetMetaData md = rs.getMetaData();
			while (rs.next()) {
				for (int i = 0; i < md.getColumnCount(); i++) {
					String colName = md.getColumnLabel(i + 1);
					int sqlType = md.getColumnType(i + 1);
					Class<?> type = JDBCTypesUtils.jdbcTypeToJavaType(sqlType);
					Object obj = rs.getObject(colName);
					obj = DBUtils.getValueBySqlType(rs, type, colName);
					for (Field field : getWholeFields(clazz)) {
						field.setAccessible(true);
						if (colName.equals(field.getName()))
							field.set(clazz.newInstance(), obj);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return entity;
	}

	public static Object getValueBySqlType(ResultSet rs, Class<?> fieldTypeClazz, String fieldName)
			throws SQLException {
		Object value = rs.getObject(fieldName);
		if ("java.lang.String".equals(fieldTypeClazz)) {// 对应oracle的char、LONG、nvarchar2、varchar2
		} else if ("java.lang.Double".equals(fieldTypeClazz)) {// 对应oracle的binary_double
			value = Double.valueOf(value.toString());
		} else if ("java.lang.Float".equals(fieldTypeClazz)) {// 对应oracle的binary_float
			value = Float.valueOf(value.toString());
		} else if ("java.math.BigDecimal".equals(fieldTypeClazz)) {// 对应oracel的number
			if ("java.lang.Integer".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = Integer.valueOf(value.toString());
			} else if ("java.lang.Long".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = Long.valueOf(value.toString());
			} else if ("java.lang.Double".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = Double.valueOf(value.toString());
			}
		} else if ("oracle.sql.BLOB".equals(fieldTypeClazz)) {// 对应oracle的BLOB
			if ("java.io.InputStream".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = rs.getBlob(fieldName).getBinaryStream();
			}
		} else if ("oracle.sql.CLOB".equals(fieldTypeClazz)) {// 对应oracle的CLOB
			if ("java.lang.String".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = rs.getClob(fieldName).toString();
			} else if ("java.io.Reader".equalsIgnoreCase(fieldTypeClazz.getName())) {
				value = rs.getClob(fieldName).getCharacterStream();
			}
		} else if ("java.sql.Timestamp".equals(fieldTypeClazz)) {// 对应oralce
																	// date
		} else if ("[B".equals(fieldTypeClazz)) {
		} else if ("oracle.sql.TIMESTAMPTZ".equals(fieldTypeClazz)// 对应oralce的timestamp_with_tim
				|| "oracle.sql.TIMESTAMPLTZ".equals(fieldTypeClazz)// 对应oralce的timestamp_with_local
				|| "oracle.sql.TIMESTAMP".equals(fieldTypeClazz)) {// 对应oracel的timestamp

			if ("java.sql.Timestamp".equals(fieldTypeClazz.getName())) {
				value = rs.getTimestamp(fieldName);
			}
		}
		return value;
	}

	/**
	 * 获取所以的类变量，包括该类的父类的类变量
	 * 
	 * @param clazz
	 * @return
	 */
	public Field[] getWholeFields(Class<?> clazz) {
		Field[] result = clazz.getDeclaredFields();
		Class<?> superClass = clazz.getSuperclass();
		while (superClass != null) {
			Field[] tempField = superClass.getDeclaredFields();
			Field[] tempResult = new Field[result.length + tempField.length];
			for (int i = 0; i < result.length; i++) {
				tempResult[i] = result[i];
			}
			for (int i = 0; i < tempField.length; i++) {
				tempResult[result.length + i] = tempField[i];
			}
			result = tempResult;
			superClass = superClass.getSuperclass();
		}
		return result;
	}
}
