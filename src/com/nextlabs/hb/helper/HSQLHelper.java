package com.nextlabs.hb.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.crypt.IDecryptor;
import com.bluejungle.framework.crypt.ReversibleEncryptor;

public class HSQLHelper {

	private static final Log LOG = LogFactory.getLog(HSQLHelper.class);

	String connectionUrl;
	String userName;
	String password;
	SimpleDateFormat dt;

	private IDecryptor decryptor = new ReversibleEncryptor();

	public HSQLHelper(String connectionUrl, String userName, String password,
			String dateFormat) {

		this.connectionUrl = connectionUrl;
		this.userName = userName;

		LOG.info("before decrypting password" + password);

		this.password = decryptor.decrypt(password);

		try {

			Class.forName("org.hsqldb.jdbc.JDBCDriver");

		} catch (ClassNotFoundException e) {

			LOG.error("HSQLHelper loading driver error: ", e);

		}

		dt = new SimpleDateFormat(dateFormat);
	}

	public HSQLHelper(String connectionUrl, String userName, String password) {

		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = decryptor.decrypt(password);

		LOG.info("before decrypting password" + password);

		try {

			Class.forName("org.hsqldb.jdbc.JDBCDriver");

		} catch (ClassNotFoundException e) {

			LOG.error("HSQLHelper loading driver error: ", e);

		}

	}

	public Connection openConnection() {
		Connection hsqlConnection = null;
		try {
			hsqlConnection = DriverManager.getConnection(connectionUrl,
					userName, password);
			hsqlConnection.setAutoCommit(true);
			LOG.info(" Connection Established with HSQL");
		} catch (SQLException e) {
			LOG.error("HSQLHelper connectToHsql() error: ", e);
			return null;
		}
		return hsqlConnection;
	}

	public void closeConnection(Connection hsqlConnection) {
		try {
			if (!hsqlConnection.isClosed())
				hsqlConnection.close();
		} catch (SQLException e) {
			LOG.error("HSQLHelper closeConnection() error: ", e);
		}
	}

}
