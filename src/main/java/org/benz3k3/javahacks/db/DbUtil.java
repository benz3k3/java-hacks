package org.benz3k3.javahacks.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DbUtil {

	public static void insertDummyRecords(String table, long count, long startId) {

		Random random = new Random(234);

		Connection conn = null;
		PreparedStatement ps = null, psw = null;
		try {
			conn = DbConfig.createConnection();
			String insert = "insert into " + table + " (id, name, passport_number) values(?, ?, ?)";
			psw = conn.prepareStatement(insert);

			while (count > 0) {
				psw.setLong(1, startId++);
				byte[] randomBytes = new byte[20];
				random.nextBytes(randomBytes);
				String name = String.valueOf(randomBytes);
				psw.setString(2, name);
				psw.setString(3, "P" + name);

				psw.execute();
				count--;
			}

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbConfig.close(ps);
			DbConfig.close(psw);
			DbConfig.close(conn);
		}
	}

	public static void main(String[] args) {
		insertDummyRecords("student", 1000000, 10000000l);
		copy("student", "old_student");

	}

	public static void copy(String sourceTable, String destinationTable) {

		long batchSize = 1000;

		Connection conn = null;
		Statement stmt = null;
		try {
			conn = DbConfig.createConnection();
			stmt = conn.createStatement();
			String sql = String.format("select count(*) from " + sourceTable);
			ResultSet rs = stmt.executeQuery(sql);
			long tableCount = 0;
			if (rs.next()) {
				tableCount = rs.getLong(1);
			}

			ExecutorService executor = Executors.newFixedThreadPool(10);
	        for (int i = 0; i <= tableCount / batchSize; i++) {
	            Runnable worker = new DbThread(i * batchSize, batchSize, sourceTable, destinationTable);
	            executor.execute(worker);
	          }
	        executor.shutdown();
	        while (!executor.isTerminated()) {
	        }
	        

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbConfig.close(stmt);
			DbConfig.close(conn);
		}

	}
}

class DbConfig {

	static final String JDBC_DRIVER = "org.h2.Driver";
	static final String DB_URL = "jdbc:h2:tcp://localhost/~/test";
	static final String USER = "sa";
	static final String PASS = "";

	public static Connection createConnection() throws ClassNotFoundException, SQLException {
		Class.forName(DbConfig.JDBC_DRIVER);
		return DriverManager.getConnection(DbConfig.DB_URL, DbConfig.USER, DbConfig.PASS);
	}

	public static void close(AutoCloseable ac) {
		try {
			if (ac != null)
				ac.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

class DbThread implements Runnable {

	private long batchStart;
	private long batchSize;
	private String sourceTable;
	private String destinationTable;

	public DbThread(long batchStart, long batchSize, String sourceTable, String destinationTable) {
		this.batchStart = batchStart;
		this.batchSize = batchSize;
		this.sourceTable = sourceTable;
		this.destinationTable = destinationTable;
	}

	public void run() {

		Connection conn = null;
		PreparedStatement ps = null, psw = null;
		try {
			conn = DbConfig.createConnection();
			ps = conn.prepareStatement("select * from " + sourceTable + " limit ? offset ?");
			ps.setLong(1, batchSize);
			ps.setLong(2, batchStart);
			ResultSet rs = ps.executeQuery();

			String insert = "insert into " + destinationTable + " (id, name, passport_number) values(?,  ?, ?)";
			psw = conn.prepareStatement(insert);

			while (rs.next()) {
				psw.setLong(1, rs.getLong(1));
				psw.setString(2, rs.getString(2));
				psw.setString(3, rs.getString(3));

				psw.addBatch();
			}
			psw.executeBatch();

			ps.close();
			psw.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbConfig.close(ps);
			DbConfig.close(psw);
			DbConfig.close(conn);
		}
	}
}
