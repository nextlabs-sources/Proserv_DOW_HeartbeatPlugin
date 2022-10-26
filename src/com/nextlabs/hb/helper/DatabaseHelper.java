package com.nextlabs.hb.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.crypt.IDecryptor;
import com.bluejungle.framework.crypt.ReversibleEncryptor;

public class DatabaseHelper {

	private static final Log LOG = LogFactory.getLog(DatabaseHelper.class);

	public Connection openConnection() {

		Connection con;

		String url = PluginConstants.commonProps.getProperty("database_url");
		String user = PluginConstants.commonProps.getProperty("database_user");
		
		IDecryptor decryptor = new ReversibleEncryptor();
		String pwd = PluginConstants.commonProps.getProperty("database_pwd");
		LOG.info("Raw password - " + pwd);
		
		String password = "";
		if(pwd!=null) {
			password = decryptor.decrypt(pwd);
			LOG.info("Decrypted password - " + password);
		}

		try {
			con = DriverManager.getConnection(url, user, password);
			LOG.debug("Connection to DB opened.");
			return con;
		} catch (SQLException e) {
			LOG.error("Error opening connection.", e);
		}

		return null;
	}

	public void closeConnection(Connection c) {

		try {
			if (!c.isClosed()) {
				c.close();
				LOG.debug("DB connection closed.");
			}
		} catch (SQLException e) {
			LOG.error("Database closeConnection() error: ", e);
		}

	}

	public Timestamp getLastUpdateTime(Connection c) {

		Timestamp lastUpdatetime = new Timestamp(0);

		Statement st = null;
		ResultSet rs = null;

		try {
			st = c.createStatement();

			String sql = "SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE database_id = DB_ID( '"
					+ PluginConstants.commonProps.getProperty("lic_db_name")
					+ "') AND OBJECT_ID=OBJECT_ID('"
					+ PluginConstants.commonProps.getProperty("lic_table_name")
					+ "')";

			if (LOG.isDebugEnabled()) {
				LOG.debug("SQL - " + sql);
			}

			rs = st.executeQuery(sql);

			while (rs.next()) {
				lastUpdatetime = rs.getTimestamp(1);
			}

		} catch (SQLException ex) {

			LOG.error("Error getting table details.", ex);

		} finally {

			try {

				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}

			} catch (SQLException ex) {

				LOG.error("Error closing transactions.", ex);

			}
		}

		return lastUpdatetime;
	}

	public List<List<String>> getLoaTable(Connection c) {

		Statement st = null;
		ResultSet rs = null;

		List<List<String>> rows = new ArrayList<List<String>>();

		try {

			st = c.createStatement();

			String sql = "SELECT "
					+ PluginConstants.commonProps
							.getProperty("lic_table_license_column_heading")
					+ ", "
					+ PluginConstants.commonProps
							.getProperty("lic_table_loa_column_heading")
					+ ", "
					+ PluginConstants.commonProps
							.getProperty("lic_table_eccn_column_heading")
					+ ", "
					+ PluginConstants.commonProps
							.getProperty("lic_table_expiry_column_heading")
					+ " FROM "
					+ PluginConstants.commonProps.getProperty("lic_table_name")
					+ ";";

			if (LOG.isDebugEnabled()) {
				LOG.debug("SQL - " + sql);
			}

			rs = st.executeQuery(sql);

			while (rs.next()) {

				List<String> row = new ArrayList<String>();

				final String license = rs.getString(1);
				final String loa = rs.getString(2);
				final String eccn = rs.getString(3);
				final String expiry = rs.getDate(4).toString();

				row.add(license);
				row.add(loa);
				row.add(eccn);
				row.add(expiry);

				rows.add(row);
			}

		} catch (SQLException ex) {

			LOG.error("Error getting table details.", ex);

		} finally {

			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}

			} catch (SQLException ex) {

				LOG.error("Error closing transactions.", ex);

			}
		}

		return rows;

	}

}
