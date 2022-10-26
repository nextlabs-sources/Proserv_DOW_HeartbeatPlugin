package com.nextlabs.hb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerAcl.AclFormatException;

import com.bluejungle.framework.comp.ComponentInfo;
import com.bluejungle.framework.comp.ComponentManagerFactory;
import com.bluejungle.framework.comp.LifestyleType;
import com.bluejungle.framework.heartbeat.HeartbeatManagerImpl;
import com.bluejungle.framework.heartbeat.IHeartbeatManager;
import com.bluejungle.framework.heartbeat.ServerHeartbeatManagerImpl;
import com.bluejungle.framework.utils.SerializationUtils;
import com.bluejungle.pf.domain.destiny.serviceprovider.IHeartbeatServiceProvider;
import com.nextlabs.hb.helper.HSQLHelper;
import com.nextlabs.hb.helper.PluginConstants;
import com.nextlabs.hb.helper.PropertyLoader;
import com.nextlabs.hb.helper.Utils;

/**
 * The DowHeartbeatPluginClient initiates a heartbeat request for license and
 * loa data to the server and processes the response. The request consists of a
 * date value, which represents the last successful update.
 * 
 * The client also maintains a HSQL in-memory database and on receiving the data
 * from the server, the client checks the validity of the data and inserts them
 * into the relevant tables.
 * 
 * NOTE : The tables are cleared before the new records are inserted. This may
 * take a few seconds (based on amount of data) during which the policy may not
 * work as expected.
 * 
 * @author pbalaji
 * 
 */

public class DowHeartbeatPluginClient implements IHeartbeatServiceProvider {

	private static final Log LOG = LogFactory
			.getLog(DowHeartbeatPluginClient.class);

	private long startTime;
	private HSQLHelper hsqlHelper;
	private static Date pcRequestDate;

	/**
	 * Initializes and registers the heartbeat plugin with the component manager
	 * and initializes the HSQL database.
	 */

	@Override
	public void init() {

		LOG.info("DHBP : " + "DowHeartbeatPluginClient - initializing.");

		/*
		 * Register the heartbeat plugin.
		 */
		final ComponentInfo<ServerHeartbeatManagerImpl> heartbeatinfo = new ComponentInfo<ServerHeartbeatManagerImpl>(
				IHeartbeatManager.class.getName(),
				HeartbeatManagerImpl.class.getName(),
				IHeartbeatManager.class.getName(), LifestyleType.SINGLETON_TYPE);
		final IHeartbeatManager heartbeatManager = (IHeartbeatManager) ComponentManagerFactory
				.getComponentManager().getComponent(heartbeatinfo);
		heartbeatManager.register(PluginConstants.NAME, this);

		/*
		 * Load property files.
		 */
		PluginConstants.installLocation = findInstallFolder();
		PluginConstants.commonProps = PropertyLoader
				.loadProperties(PluginConstants.installLocation
						+ PluginConstants.PCROOT + PluginConstants.JARFOLDER
						+ PluginConstants.APPFOLDER + "/Common_HB.Properties");
		PluginConstants.pluginProps = PropertyLoader
				.loadProperties(PluginConstants.installLocation
						+ PluginConstants.PCROOT + PluginConstants.CONFFOLDER
						+ "/DowHeartbeatPluginClient_HB.properties");
		
		/*
		 * Initialize and start HSQL DB.
		 */
		try {
			final String path = PluginConstants.installLocation
					+ PluginConstants.PCROOT + PluginConstants.JARFOLDER
					+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER;
			LOG.info("DHBP : " + "Starting in memory database. Path - " + path);
			startInmemoryDB(path, PluginConstants.PCINMEMDB);
		} catch (IOException e) {
			LOG.error("DHBP : " + "Error while starting in-memory database. ", e);
		} catch (AclFormatException e) {
			LOG.error("DHBP : " + "Error while starting in-memory database. ", e);
		}

		hsqlHelper = new HSQLHelper(
				PluginConstants.commonProps.getProperty("hsql_server_url"),
				PluginConstants.commonProps.getProperty("hsql_user_name"),
				PluginConstants.commonProps.getProperty("hsql_password"));

		/*
		 * Initialize date for initial request.
		 */
		pcRequestDate = new Date(0, 0, 1);

		LOG.info("DHBP : " + "DowHeartbeatPluginClient - initialized");

	}

	/**
	 * Prepares a request to be sent to the control center. The data sent is a
	 * HashMap which contains a Date object. This date is initially set to Jan
	 * 1, 1970 and is subsequently updated on every successful request-response
	 * cycle.
	 * 
	 * A past date ensures that the policy controller gets data from the server
	 * when it is started.
	 * 
	 * @param name
	 *            name of the HeartBeat plugin as registered with the component
	 *            manager.
	 * @return a HashMap with the request date.
	 */

	@Override
	public Serializable prepareRequest(final String name) {

		/*
		 * Check if the name argument matches the plugin name. If it does not
		 * match, exit.
		 */

		if (!name.equals(PluginConstants.NAME)) {
			return null;
		}

		/*
		 * Start counting to measure time taken for a complete request-response
		 * cycle.
		 */

		startTime = System.nanoTime();

		/*
		 * Prepare request.
		 */

		final HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("Date", pcRequestDate);

		LOG.info("DHBP : " + "Preparing Request - " + map);

		return map;

	}

	/**
	 * Receives the response from the server and processes the data accordingly.
	 * Multiple combinations are dealt with here.
	 * 
	 * If the server returns NULL, the data on the Policy Controller is upto
	 * date with the content on the server. Hence, no update is done on the
	 * Policy Controller.
	 * 
	 * If the server sends back data, the HashMap received is checked for flags
	 * which indicate the files that are sent. Correspondingly, the files are
	 * extracted from the zip file and the HSQL database is updated.
	 * 
	 * It is possible that only one of the two files are sent. This is decided
	 * by the server based on the last modified dates of the database tables as
	 * well as the PC request date sent by prepareRequest method.
	 * 
	 * @param name
	 *            name of the HeartBeat plugin as registered with the component
	 *            manager.
	 * @param resposnse
	 *            HashMap containing data from the server, or NULL. *
	 */

	
	public void processResponse(final String name, final Serializable response) {

		/*
		 * Check if the name argument matches the plugin name. If it does not
		 * match, exit.
		 */

		if (!name.equals(PluginConstants.NAME)) {
			return;
		}

		/*
		 * If response is null, no update. Exit.
		 */

		if (response == null) {

			LOG.info("DHBP : " + "Response is null! No update!");

		} else {

			/*
			 * Got response from server. Update the PC request time for future
			 * requests and process the received data.
			 */

			pcRequestDate = new Date();

			final HashMap<String, Object> returnData = (HashMap<String, Object>) response;

			LOG.info("DHBP : " + "Got response from server - " + response.toString());

			/*
			 * Check if the HashMap contains valid data. If yes, extract the zip
			 * file and process the data.
			 */

			if (returnData.get("Data") != null) {

				/*
				 * The value contained for key "Data" is a zipped file. Extract
				 * the file into a folder.
				 */

				final byte[] bytes = (byte[]) returnData.get("Data");

				final String path = PluginConstants.installLocation
						+ PluginConstants.PCROOT + PluginConstants.JARFOLDER
						+ PluginConstants.APPFOLDER
						+ PluginConstants.compressedFileName;
				final String extractTo = PluginConstants.installLocation
						+ PluginConstants.PCROOT + PluginConstants.JARFOLDER
						+ PluginConstants.APPFOLDER;
				
				LOG.info("DHBP : " + "Zip file path : " + path);
				LOG.info("DHBP : " + "Zip file Extract path : " + extractTo);

				final File file = new File(path);

				try {

					final FileOutputStream fos = new FileOutputStream(file);
					fos.write(bytes);
					fos.close();

				} catch (IOException e) {

					LOG.error("DHBP : " + 
							"Exception in writing the compressed response to the file:",
							e);

				}

				Utils.uncompressFiles(extractTo, file);
				LOG.debug("DHBP : " + "Extracted zip file.");

				/*
				 * Get paths for both files.
				 */

				final String dictDataFilePath = extractTo + "/"
						+ PluginConstants.dictDataFileName;
				final String loaEccnFilePath = extractTo + "/"
						+ PluginConstants.loaEccnFileName;

				if (LOG.isDebugEnabled()) {
					LOG.debug("DHBP : " + "DictData file path - " + dictDataFilePath);
					LOG.debug("DHBP : " + "LoaEccn file path - " + loaEccnFilePath);
				}

				Statement statement = null;
				Connection hsqlConn = hsqlHelper.openConnection();

				/*
				 * Check if Dictionary Data is returned. If yes, process the
				 * data and update the USERLIC and USER LOA tables.
				 */

				if (returnData.get("dictFile").equals("YES")) {

					final List<HashMap<String, String>> dictData = (List<HashMap<String, String>>) Utils
							.readData(dictDataFilePath);

					LOG.info("DHBP : " + "DictData extracted.");

					/*
					 * Do NOT update the tables if the dictionaryData is
					 * corrupt, NULL or empty. This makes sure that the tables
					 * on the Policy Controller are not updated with bad data.
					 */

					if (dictData != null && !dictData.isEmpty()) {
						
						Set<String> uidSet = new HashSet<String>();
						
						for(HashMap<String, String> map : dictData) {
							uidSet.add(map.get("UID"));
						}
						
						LOG.info("DHBP : " + "Dictionary data is not null. Contains "
								+ uidSet.size() + " records.");
						LOG.info("DHBP : " + "Updating USERLIC and USERLOA tables.");

						try {

							statement = hsqlConn.createStatement();

							final long updateStartTime = System.nanoTime();

							String sqlStatement = "DELETE FROM USERLOA;";
							statement.executeUpdate(sqlStatement);

							sqlStatement = "DELETE FROM USERLIC;";
							statement.executeUpdate(sqlStatement);

							for (final HashMap<String, String> data : dictData) {
								try
								{
									if (!data.containsKey("LOAS") && data.containsKey("UID") && data.containsKey("LICENSES")) {
										if(data.get("UID")!=null && data.get("LICENSES")!=null) {
											sqlStatement = "INSERT INTO USERLIC VALUES('"
													+ data.get("UID").toLowerCase()
													+ "','"
													+ data.get("LICENSES").toLowerCase() + "');";
											statement.executeUpdate(sqlStatement);
										}
										else
										{
											LOG.error("DHBP : " + "UserID or License or both is null. Data is  : " + data.toString());
										}
									}
									if (!data.containsKey("LICENSES") && data.containsKey("LOAS") && data.containsKey("UID")) {
										if(data.get("UID")!=null && data.get("LOAS")!=null) {
											sqlStatement = "INSERT INTO USERLOA VALUES('"
													+ data.get("UID").toLowerCase()
													+ "','"
													+ data.get("LOAS").toLowerCase() + "');";
											statement.executeUpdate(sqlStatement);
										}
										else
										{
											LOG.error("DHBP : " + "UserID or LOA or both is null. Data is  : " + data.toString());
										}
									}
								} catch (Exception e) {
									LOG.error("DHBP : " + "Error in updating USERLIC/USERLOA for record : " + data.toString());
								}
							}

							LOG.info("DHBP : " + "Updating user tables took "
									+ (System.nanoTime() - updateStartTime)
									/ 1000000 + " ms.");

						} catch (SQLException e) {
							LOG.error("DHBP : " + "Error while inserting into HSQL. ", e);							
						} finally {
							if (statement != null) {
								try {
									statement.close();
								} catch (SQLException e) {
									LOG.error("DHBP : " + "Cant close statement", e);
								}
							}

							if (hsqlConn != null) {
								try {
									hsqlConn.close();
								} catch (SQLException e) {
									LOG.error("DHBP : " + "Cant close hsql connection.", e);
								}
							}
						}
					} else {
						LOG.info("DHBP : " + "Dictionary data is NULL or of zero size. Not updating USERLIC & USERLOA tables.");
					}
				}

				/*
				 * Check if LOA-ECCN data is returned by the server. If yes,
				 * update the LOA table on the HSQL database. *
				 */

				if (returnData.get("loaFile").equals("YES")) {

					final List<HashMap<String, String>> loaEccnData = (List<HashMap<String, String>>) Utils
							.readData(loaEccnFilePath);
					LOG.info("DHBP : " + "LoaEccn data extracted.");

					hsqlConn = hsqlHelper.openConnection();

					if (loaEccnData != null && !loaEccnData.isEmpty()) {

						LOG.info("DHBP : " + "LOA ECCN data is not null. Contains "
								+ loaEccnData.size() + " records.");
						LOG.info("DHBP : " + "Updating LOAECCN table.");

						try {

							statement = hsqlConn.createStatement();

							final long updateStartTime = System.nanoTime();

							String sqlStatement = "DELETE FROM LOADB;";
							statement.executeUpdate(sqlStatement);

							for (final HashMap<String, String> data : loaEccnData) {
								try{
									String insertStmt = "INSERT INTO LOADB VALUES ('"
											+ data.get("LICENSE").toLowerCase() + "','" + data.get("LOA").toLowerCase()
											+ "','" + data.get("ECCN").toLowerCase() + "','"
											+ data.get("EXPIRY") + "','" + data.get("EFFECTIVE").toLowerCase()
											+ "');";
									statement.executeUpdate(insertStmt);
								} catch (Exception e) {
									LOG.error("DHBP : " + "Error in updating LOADB for record : " + data.toString());
								}
							}
							
							LOG.info("DHBP : " + "Updating LOA table took "
									+ (System.nanoTime() - updateStartTime)
									/ 1000000 + " ms.");
						} catch (SQLException e) {
							LOG.error("DHBP : " + "Error while inserting into HSQL.", e);							
						} finally {
							if (statement != null) {
								try {
									statement.close();
								} catch (SQLException e) {
									LOG.error("DHBP : " + "Cant close statement.", e);
								}
							}

							if (hsqlConn != null) {
								try {
									hsqlConn.close();
								} catch (SQLException e) {
									LOG.error("DHBP : " + "Cant close hsql connection.", e);
								}
							}
						}
					}else {
						LOG.info("DHBP : " + "LOA-ECCN data is NULL or of zero size. Not updating LOADB table.");
					}
				}
			}
		}
		
		int dictCount = -1;
		int licLoaEccnCount = -1;
		
		Connection hsqlConn = null;
		Statement stmt = null;
		
		try {
			hsqlConn = hsqlHelper.openConnection();
			stmt = hsqlConn.createStatement();
			
			ResultSet userQueryRs = stmt.executeQuery("SELECT COUNT(DISTINCT UID) AS UIDCOUNT FROM (SELECT UID FROM USERLIC UNION SELECT UID FROM USERLOA);");
			while(userQueryRs.next()){
				dictCount = Integer.parseInt(userQueryRs.getString("UIDCOUNT"));
		    }
		} catch (SQLException e) {
			LOG.error("DHBP : " + "Error querying HSQL DB", e);
		} finally {
			if(stmt!=null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					LOG.error("DHBP : " + "Error closing stmt.", e);
				}
			}
			if(hsqlConn!=null) {
				try {
					hsqlConn.close();
				} catch (SQLException e) {
					LOG.error("DHBP : " + "Error closing conn.", e);
				}
			}
		}
		
		try {
			hsqlConn = hsqlHelper.openConnection();
			stmt = hsqlConn.createStatement();
			
			ResultSet userQueryRs = stmt.executeQuery("SELECT COUNT(*) AS UIDCOUNT FROM LOADB;");
			while(userQueryRs.next()){
				licLoaEccnCount = Integer.parseInt(userQueryRs.getString("UIDCOUNT"));
		    }
		} catch (SQLException e) {
			LOG.error("DHBP : " + "Error querying HSQL DB", e);
		} finally {
			if(stmt!=null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					LOG.error("DHBP : " + "Error closing stmt.", e);
				}
			}
			if(hsqlConn!=null) {
				try {
					hsqlConn.close();
				} catch (SQLException e) {
					LOG.error("DHBP : " + "Error closing conn.", e);
				}
			}
		}
		
		LOG.info("DHBP : " + "Database contains " + String.valueOf(dictCount) + " dictionary records and " + String.valueOf(licLoaEccnCount) + " License-LOA-ECCN records.");
		
		LOG.info("DHBP : " + "DowHeartbeatPluginClient total request-response time - "
				+ ((System.nanoTime() - startTime) / 1000000.00) + " ms");
	}

	/**
	 * Gets the installation path.
	 * 
	 * @return String containing absolute installation path.
	 */

	private String findInstallFolder() {

		final File file = new File(".");
		return file.getAbsolutePath();

	}

	/**
	 * Starts the in memory HSQL database using the path and the database name
	 * given as parameters.
	 * 
	 * @param path
	 *            path in which the database is created.
	 * @param dbName
	 *            name of the in memory database.
	 * @throws IOException
	 * @throws AclFormatException
	 */

	private void startInmemoryDB(final String path, final String dbName)
			throws IOException, AclFormatException {

		final HsqlProperties hProps = new HsqlProperties();
		hProps.setProperty("server.database.0", "file:/" + path + dbName);
		hProps.setProperty("server.dbname.0", dbName);
		LOG.info("DHBP : " + "HSQL DB Name - " + dbName);

		final Server server = new Server();
		server.setProperties(hProps);
		server.setLogWriter(null);
		server.setErrWriter(null);
		server.start();

	}
	
	/**
	 * New HeartBeat Implementation Interface  from CC 6.5
	 */
	public void processResponse(String name, String data) {
		if (data != null) {
			processResponse(name, SerializationUtils.unwrapSerialized(data));
		} else {
			LOG.warn("Data is null: No Update from the HeartBeat Plugin");
		}
	}
}
