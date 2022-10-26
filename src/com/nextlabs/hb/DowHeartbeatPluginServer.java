package com.nextlabs.hb;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.destiny.container.dcc.plugin.IDCCHeartbeatServerPlugin;
import com.bluejungle.destiny.server.shared.registration.IRegisteredDCCComponent;
import com.bluejungle.framework.comp.ComponentInfo;
import com.bluejungle.framework.comp.ComponentManagerFactory;
import com.bluejungle.framework.comp.LifestyleType;
import com.bluejungle.framework.heartbeat.IServerHeartbeatManager;
import com.bluejungle.framework.heartbeat.ServerHeartbeatManagerImpl;
import com.bluejungle.framework.utils.SerializationUtils;
import com.nextlabs.hb.helper.DictionaryData;
import com.nextlabs.hb.helper.PluginConstants;
import com.nextlabs.hb.helper.PropertyLoader;
import com.nextlabs.hb.helper.Utils;

/** DowHeartbeatPluginServer class is used to collect and transmit licensing
 * information from the server to the client. The information is collected from
 * two areas, namely - the enrolled data (dictionary database) and a CSV file
 * which contains license, loa and eccn mappings along with effective & expiry
 * dates.
 * 
 * This server side plugin selectively transfers data to the client based on
 * multiple date values - the PC request date, the file modified date and the
 * database last updated date. Hence, the architecture is suitable for multiple
 * PC scenarios and also conserves bandwidth by not sending data when the client
 * is up to date.
 * 
 * Many of the parameters, including the database connection string, the table
 * column name mapping etc, are configurable through the property files.
 * 
 * @author pbalaji */
public class DowHeartbeatPluginServer implements IDCCHeartbeatServerPlugin {
	private static final Log LOG = LogFactory
			.getLog(DowHeartbeatPluginServer.class);

	private enum decision {
		NONE, SENDFILE, UPDATEFILEANDSEND
	};

	/** Initializes and registers the HeartBeat plugin with the COntrol Center
	 * Component Manager. This method also loads the property files from their
	 * respective locations for use in other functions. */
	@Override
	public void init(IRegisteredDCCComponent component) {
		LOG.info("DHBP : " + "DowHeartbeatPluginServer -- Initializing.");
		/* Load property files. */
		PluginConstants.installLocation = findInstallFolder();
		PluginConstants.commonProps = PropertyLoader
				.loadProperties(PluginConstants.installLocation
						+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
						+ PluginConstants.APPFOLDER + "/Common_HB.Properties");
		PluginConstants.pluginProps = PropertyLoader
				.loadProperties(PluginConstants.installLocation
						+ PluginConstants.CCROOT + PluginConstants.CONFFOLDER
						+ "/DowHeartbeatPluginServer_HB.properties");
		/* Register the heartbeat plugin. */
		final ComponentInfo<ServerHeartbeatManagerImpl> heartbeatManagerComp = new ComponentInfo<ServerHeartbeatManagerImpl>(
				IServerHeartbeatManager.COMP_NAME,
				ServerHeartbeatManagerImpl.class,
				IServerHeartbeatManager.class, LifestyleType.SINGLETON_TYPE);
		final IServerHeartbeatManager heartbeatMgr = ComponentManagerFactory
				.getComponentManager().getComponent(heartbeatManagerComp);
		heartbeatMgr.register(PluginConstants.NAME, this);
		try {
			LOG.info("DHBP : " + "Creating Dictionary Data file.");
			List<HashMap<String, String>> dictionaryData = getDictionaryData();
			if (dictionaryData != null && !dictionaryData.isEmpty()) {
				writeDictionaryData(dictionaryData);
			}
		} catch (Exception e) {
			LOG.error("DHBP : " + "Error creating Dictionary Data file.", e);
		}
		try {
			LOG.info("DHBP : " + "Creating LIC-LOA-ECCN Data file.");
			List<HashMap<String, String>> licLoaEccnData = getLicLoaEccnData();
			if (licLoaEccnData != null && !licLoaEccnData.isEmpty()) {
				writeLicLoaEccnData(licLoaEccnData);
			}
		} catch (Exception e) {
			LOG.error("DHBP : " + "Error creating Dictionary Data file.", e);
		}
		LOG.info("DHBP : " + "DowHeartbeatPlugin is registered by component - "
				+ component.getComponentName());
		LOG.info("DHBP : " + "DowHeartbeatPluginServer -- Initialized.");
	}

	/** Finds the installation directory.
	 * 
	 * @return path of the installation directory. */
	private String findInstallFolder() {
		String path = this.getClass().getProtectionDomain().getCodeSource()
				.getLocation().getPath();
		try {
			path = URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error("DHBP : " + "Exeception decoding file path : ", e);
		}
		final int endIndex = path.indexOf("/server/plugins");
		path = path.substring(1, endIndex);
		return path;
	}

	/** Compares three dates - PC Request Date, File Last Modified Date and the
	 * Database Table Last Updated Date to decide the response of the HeartBeat
	 * plugin. The possible responses are NONE, SENDFILE and
	 * UPDATEDBANDSENDFILE.
	 * 
	 * If the PC Request Date is the latest, then NONE is returned.
	 * 
	 * If File Modified Date is the latest, SENDFILE is returned.
	 * 
	 * If the DB Table Modified Time is the latest, UPDATEFILEANDSEND is
	 * returned.
	 * 
	 * If one or more dates are null, then the a decision is made with the
	 * remaining two dates.
	 * 
	 * If two dates or more are null, NONE is returned.
	 * 
	 * @param pcDate PolicyController request date.
	 * @param fileDate Last modified date for the .bin file.
	 * @param dbDate Last modified date for the database table.
	 * @return an object of decision enum. */
	private decision makeDecision(java.util.Date pcDate,
			java.util.Date fileDate, java.util.Date dbDate) {
		if (pcDate != null && fileDate != null && dbDate != null) {
			if (!pcDate.before(fileDate) && !pcDate.before(dbDate)) {
				return decision.NONE;
			}
			if (!fileDate.before(pcDate) && !fileDate.before(dbDate)) {
				return decision.SENDFILE;
			}
			if (!dbDate.before(pcDate) && !dbDate.before(fileDate)) {
				return decision.UPDATEFILEANDSEND;
			}
		} else if (pcDate == null && fileDate != null && dbDate != null) {
			if (!fileDate.before(dbDate)) {
				return decision.SENDFILE;
			} else {
				return decision.UPDATEFILEANDSEND;
			}
		} else if (pcDate != null && fileDate == null && dbDate != null) {
			if (!pcDate.before(dbDate)) {
				return decision.NONE;
			} else {
				return decision.UPDATEFILEANDSEND;
			}
		} else if (pcDate != null && fileDate != null && dbDate == null) {
			if (!pcDate.before(fileDate)) {
				return decision.NONE;
			} else {
				return decision.SENDFILE;
			}
		}
		return decision.NONE;
	}

	private boolean validate(String parameter, String propertyName,
			int defaultLength) {
		boolean isValid = false;
		int parameterLength = defaultLength;
		try {
			String propertyString = PluginConstants.pluginProps
					.getProperty(propertyName);
			if (propertyString != null && !propertyString.isEmpty()) {
				parameterLength = Integer.parseInt(propertyString.trim());
			}
		} catch (Exception e) {
			LOG.error("DHBP : " + "Error getting " + propertyName
					+ " from properties file. Using default value of "
					+ String.valueOf(defaultLength) + ".");
			parameterLength = defaultLength;
		}
		if (parameter != null && !parameter.isEmpty()) {
			if (parameter.trim().length() <= parameterLength) {
				isValid = true;
			}
		}
		return isValid;
	}

	private boolean validateLoa(String loa) {
		boolean isValid = false;
		isValid = validate(loa, "loa_length", 7);
		return isValid;
	}

	private boolean validateLicense(String license) {
		boolean isValid = false;
		isValid = validate(license, "license_length", 9);
		return isValid;
	}

	private boolean validateEccn(String eccn) {
		boolean isValid = false;
		isValid = validate(eccn, "eccn_length", 10);
		return isValid;
	}

	private List<String> getPipeSeparatedStrings(String sourceString) {
		List<String> separatedString = new ArrayList<String>();
		if (sourceString != null) {
			String[] stringElements = sourceString.split("\\|");
			for (String stringElement : stringElements) {
				if (stringElement != null && !stringElement.isEmpty()) {
					separatedString.add(stringElement.trim());
				}
			}
		}
		return separatedString;
	}

	private List<HashMap<String, String>> getDictionaryData() {
		LOG.info("DHBP : " + "Getting data from Dictionary Database.");
		boolean isError = false;
		boolean errorFlag = false;
		int totalCount = 0;
		int validCount = 0;
		int licLoaCount = 0;
		StringBuilder logContent = new StringBuilder();
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Date date = new Date();
		logContent.append("Last update time : " + dateFormat.format(date)
				+ "\r\n");
		List<HashMap<String, String>> userData = DictionaryData.getUserData();
		List<HashMap<String, String>> dictionaryData = new ArrayList<HashMap<String, String>>();
		LOG.debug("DHBP : " + "Dictionary DB contains "
				+ String.valueOf(userData.size()) + " records.");
		if (userData != null) {
			String userIdFieldName = PluginConstants.commonProps
					.getProperty("userid_dict_field");
			if (userIdFieldName == null || userIdFieldName.isEmpty()) {
				LOG.error("DHBP : "
						+ "User ID field name is NULL or empty. Check Common_HB.properties file.");
			}
			String loaFieldName = PluginConstants.commonProps
					.getProperty("loa_dict_field");
			if (loaFieldName == null || loaFieldName.isEmpty()) {
				LOG.error("DHBP : "
						+ "LOA field name is NULL or empty. Check Common_HB.properties file.");
			}
			String licenseFieldName = PluginConstants.commonProps
					.getProperty("license_dict_field");
			if (licenseFieldName == null || licenseFieldName.isEmpty()) {
				LOG.error("DHBP : "
						+ "License field name is NULL or empty. Check Common_HB.properties file.");
			}
			for (final HashMap<String, String> user : userData) {
				isError = false;
				if (LOG.isDebugEnabled()) {
					LOG.info("DHBP : " + "Dictionary Record : "
							+ user.toString());
				}
				String userId = user.get(userIdFieldName);
				String loa = user.get(loaFieldName);
				String license = user.get(licenseFieldName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("DHBP : " + "Username 	: " + userId);
					LOG.debug("DHBP : " + "LOA 		: " + loa);
					LOG.debug("DHBP : " + "License 	: " + license);
				}
				if (userId == null || userId.isEmpty()) {
					logContent.append("Enrollment : " + user.get("Enrollment")
							+ " - " + userIdFieldName
							+ " is NULL or empty. Current Record is : "
							+ user.toString() + "\r\n");
					isError = true;
					LOG.error("DHBP : " + "Enrollment : "
							+ user.get("Enrollment") + " - " + userIdFieldName
							+ " is NULL or empty. Current Record is : "
							+ user.toString());
				}
				if (!((loa == null || loa.isEmpty()) && (license == null || license
						.isEmpty()))) {
					validCount++;
				}
				if (license != null && !license.isEmpty()) {
					List<String> licenses = getPipeSeparatedStrings(license);
					for (String licenseElement : licenses) {
						if (validateLicense(licenseElement)) {
							HashMap<String, String> dictionaryRecord = new HashMap<String, String>();
							dictionaryRecord.put("UID", userId);
							dictionaryRecord.put("LICENSES", licenseElement);
							dictionaryData.add(dictionaryRecord);
						} else {
							logContent.append("Enrollment : "
									+ user.get("Enrollment")
									+ " - Invalid License : " + licenseElement
									+ "\r\n");
							isError = true;
							LOG.error("DHBP : " + "Enrollment : "
									+ user.get("Enrollment")
									+ " - Invalid License : " + licenseElement);
						}
					}
				}
				if (loa != null && !loa.isEmpty()) {
					List<String> loas = getPipeSeparatedStrings(loa);
					for (String loaElement : loas) {
						if (validateLicense(loaElement)) {
							HashMap<String, String> dictionaryRecord = new HashMap<String, String>();
							dictionaryRecord.put("UID", userId);
							dictionaryRecord.put("LOAS", loaElement);
							dictionaryData.add(dictionaryRecord);
						} else {
							logContent
									.append("Enrollment : "
											+ user.get("Enrollment")
											+ " - Invalid LOA : " + loaElement
											+ "\r\n");
							isError = true;
							LOG.error("DHBP : " + "Enrollment : "
									+ user.get("Enrollment")
									+ " - Invalid LOA : " + loaElement);
						}
					}
				}
				if (isError) {
					errorFlag = true;
				} else {
					if (!((loa == null || loa.isEmpty()) && (license == null || license
							.isEmpty()))) {
						licLoaCount++;
					}
				}
				totalCount++;
			}
		}
		if (!errorFlag) {
			logContent.append("No error found.");
		}
		String dictDataErrorLogFilePath = PluginConstants.installLocation
				+ PluginConstants.LOGROOT + "/"
				+ PluginConstants.dictDataErrorLog;
		LOG.info("DHBP : " + "Writing errors to log file at : "
				+ dictDataErrorLogFilePath);
		try {
			File file = new File(dictDataErrorLogFilePath);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(logContent.toString());
			bw.close();
		} catch (Exception e) {
			LOG.error("DHBP : "
					+ "Error writing to Doctionary DB log file at : "
					+ dictDataErrorLogFilePath + ".");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("DHBP : " + "Dictionary Database contains : "
					+ dictionaryData.toString());
		}
		LOG.info("DHBP : " + "Dictionary Database contains : "
				+ String.valueOf(dictionaryData.size()) + " valid records.");
		LOG.info("DHBP : " + "[Dictionary Database] Sending "
				+ String.valueOf(licLoaCount) + "/"
				+ String.valueOf(validCount) + " ("
				+ String.valueOf(totalCount) + ") records.");
		LOG.info("DHBP : " + "Got Dictionary Data.");
		return dictionaryData;
	}

	private void writeDictionaryData(
			List<HashMap<String, String>> dictionaryData) {
		String dictDataFilePath = PluginConstants.installLocation
				+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
				+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER
				+ PluginConstants.dictDataFileName;
		LOG.info("DHBP : " + "Writing Dictionary data to file - "
				+ dictDataFilePath);
		writeSerializableDataToFile(dictionaryData, dictDataFilePath);
		LOG.info("DHBP : " + "Wrote data to file.");
	}

	private List<HashMap<String, String>> getLicLoaEccnData() {
		LOG.info("DHBP : " + "Getting data from LIC-LOA-ECCN CSV files.");
		int totalCount = 0;
		int validCount = 0;
		StringBuilder logContent = new StringBuilder();
		DateFormat dateFormat = new SimpleDateFormat("MM/MM/yyyy HH:mm:ss");
		Date date = new Date();
		logContent.append("Last update time : " + dateFormat.format(date)
				+ "\r\n");
		String licLoaEccnFile = PluginConstants.pluginProps
				.getProperty("csv_file_path");
		logContent
				.append("CSV file is located at : " + licLoaEccnFile + "\r\n");
		LOG.info("DHBP : " + "CSV file is located at : " + licLoaEccnFile + ".");
		List<HashMap<String, String>> licLoaEccnData = new ArrayList<HashMap<String, String>>();
		List<String> licLoaEccnCombo = new ArrayList<String>();
		BufferedReader br = null;
		String line = null;
		String csvSplitBy = ",";
		boolean isHeader = true;
		boolean isError = false;
		boolean errorFlag = false;
		int lineNumber = 0;
		if (licLoaEccnFile == null || licLoaEccnFile.isEmpty()) {
			LOG.error("DHBP : " + "CSV file name not defined or empty.");
		} else {
			try {
				br = new BufferedReader(new FileReader(licLoaEccnFile));
				while ((line = br.readLine()) != null) {
					isError = false;
					if (LOG.isDebugEnabled()) {
						LOG.debug("DHBP : " + "LIC LOA ECCN Record (RAW) : "
								+ line);
					}
					lineNumber++;
					if (isHeader) {
						isHeader = false;
						continue;
					}
					String[] items = line.split(csvSplitBy);
					if (items.length != 5) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : Missing columns.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : Missing columns.");
					}
					String license = items[0].trim();
					String loa = items[1].trim();
					String eccn = items[2].trim();
					String effectiveDate = items[3].trim();
					String expiryDate = items[4].trim();
					if (license != null && !license.isEmpty()) {
						if (!validateLicense(license)) {
							logContent.append("Line "
									+ String.valueOf(lineNumber)
									+ " : Invalid License.\r\n");
							isError = true;
							LOG.error("DHBP : " + "Line "
									+ String.valueOf(lineNumber)
									+ " : Invalid License.");
						}
					}
					if (loa != null && !loa.isEmpty()) {
						if (!validateLoa(loa)) {
							logContent.append("Line "
									+ String.valueOf(lineNumber)
									+ " : Invalid LOA.\r\n");
							isError = true;
							LOG.error("DHBP : " + "Line "
									+ String.valueOf(lineNumber)
									+ " : Invalid LOA.");
						}
					}
					if ((license == null || license.isEmpty())
							&& (loa == null || loa.isEmpty())) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : License & LOA are empty or NULL.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : License & LOA are empty or NULL.");
					} else {
						if (license.isEmpty() || license == null) {
							license = "NULL";
						}
						if (loa.isEmpty() || loa == null) {
							loa = "NULL";
						}
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("DHBP : " + "LICENSE 	: " + license);
						LOG.debug("DHBP : " + "LOA     	: " + loa);
						LOG.debug("DHBP : " + "ECCN    	: " + eccn);
						LOG.debug("DHBP : " + "EFFECTIVE: " + effectiveDate);
						LOG.debug("DHBP : " + "EXPIRY	: " + expiryDate);
					}
					if (!validateEccn(eccn)) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : Invalid ECCN.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : Invalid ECCN.");
					}
					if (effectiveDate == null || effectiveDate.isEmpty()) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : Effective Date NULL or empty.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : Effective Date NULL or empty.");
					}
					if (expiryDate == null || expiryDate.isEmpty()) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : Expiry Date NULL or empty.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : Expiry Date NULL or empty.");
					}
					if (licLoaEccnCombo.contains(license.concat(loa).concat(
							eccn))) {
						logContent.append("Line " + String.valueOf(lineNumber)
								+ " : Duplicate record.\r\n");
						isError = true;
						LOG.error("DHBP : " + "Line "
								+ String.valueOf(lineNumber)
								+ " : Duplicate record.");
					} else {
						licLoaEccnCombo.add(license.concat(loa).concat(eccn));
					}
					DateFormat sourceDf = new SimpleDateFormat("MM/dd/yyyy");
					sourceDf.setLenient(false);
					DateFormat targetDf = new SimpleDateFormat("yyyy-MM-dd");
					targetDf.setLenient(false);
					String effectiveDateHsql = "";
					String expiryDateHsql = "";
					try {
						Date tempDate = sourceDf.parse(effectiveDate);
						effectiveDateHsql = targetDf.format(tempDate);
						if (effectiveDateHsql.isEmpty()) {
							logContent
									.append("Line "
											+ String.valueOf(lineNumber)
											+ " : Error parsing Effective Date. Ensure date format is MM/dd/yyyy.\r\n");
							isError = true;
							LOG.error("DHBP : "
									+ "Line "
									+ String.valueOf(lineNumber)
									+ " : Error parsing Effective Date. Ensure date format is MM/dd/yyyy.");
						}
					} catch (Exception e) {
						logContent
								.append("Line "
										+ String.valueOf(lineNumber)
										+ " : Error parsing Effective Date. Ensure date format is MM/dd/yyyy.\r\n");
						isError = true;
						LOG.error("DHBP : "
								+ "Line "
								+ String.valueOf(lineNumber)
								+ " : Error parsing Effective Date. Ensure date format is MM/dd/yyyy.");
					}
					try {
						Date tempDate = sourceDf.parse(expiryDate);
						expiryDateHsql = targetDf.format(tempDate);
						if (expiryDateHsql.isEmpty()) {
							logContent
									.append("Line "
											+ String.valueOf(lineNumber)
											+ " : Error parsing Expiry Date. Ensure date format is MM/dd/yyyy.\r\n");
							isError = true;
							LOG.error("DHBP : "
									+ "Line "
									+ String.valueOf(lineNumber)
									+ " : Error parsing Expiry Date. Ensure date format is MM/dd/yyyy.");
						}
					} catch (Exception e) {
						logContent
								.append("Line "
										+ String.valueOf(lineNumber)
										+ " : Error parsing Expiry Date. Ensure date format is MM/dd/yyyy.\r\n");
						isError = true;
						LOG.error("DHBP : "
								+ "Line "
								+ String.valueOf(lineNumber)
								+ " : Error parsing Expiry Date. Ensure date format is MM/dd/yyyy.");
					}
					try {
						Date startDate = sourceDf.parse(effectiveDate);
						Date endDate = sourceDf.parse(expiryDate);
						if (startDate.after(endDate)) {
							logContent
									.append("Line "
											+ String.valueOf(lineNumber)
											+ " : Effective date is after Expiry date.\r\n");
							isError = true;
							LOG.error("DHBP : " + "Line "
									+ String.valueOf(lineNumber)
									+ " : Effective date is after Expiry date");
						}
					} catch (Exception e) {
						// Do nothing. Incorrect date format handled above.
					}
					if (isError) {
						errorFlag = true;
					} else {
						validCount++;
					}
					totalCount++;
					if (!isError) {
						HashMap<String, String> licLoaEccnRecord = new HashMap<String, String>();
						licLoaEccnRecord.put("LICENSE", license);
						licLoaEccnRecord.put("LOA", loa);
						licLoaEccnRecord.put("ECCN", eccn);
						licLoaEccnRecord.put("EFFECTIVE", effectiveDateHsql);
						licLoaEccnRecord.put("EXPIRY", expiryDateHsql);
						licLoaEccnData.add(licLoaEccnRecord);
					}
				}
			} catch (Exception e) {
				LOG.error("DHBP : " + "Unable to read LIC-LOA-ECCN CSV file",e);
				
			}
		}
		if (!errorFlag) {
			logContent.append("No error found.");
		}
		String licLoaEccnErrorLogFilePath = PluginConstants.installLocation
				+ PluginConstants.LOGROOT + "/"
				+ PluginConstants.loaEccnCsvErrorLog;
		LOG.info("DHBP : " + "Writing errors to log file at : "
				+ licLoaEccnErrorLogFilePath);
		try {
			File file = new File(licLoaEccnErrorLogFilePath);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(logContent.toString());
			bw.close();
		} catch (Exception e) {
			LOG.error("DHBP : "
					+ "Error writing to Doctionary DB log file at : "
					+ licLoaEccnErrorLogFilePath + ".");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("DHBP : " + "LIC-LOA-ECCN CSV file contains : "
					+ licLoaEccnData.toString());
		}
		// LOG.info("DHBP : " + "LIC-LOA-ECCN CSV file contains : " +
		// String.valueOf(licLoaEccnData.size()) + " records.");
		LOG.info("DHBP : " + "[LIC-LOA-ECCN CSV] Sending "
				+ String.valueOf(validCount) + "/" + String.valueOf(totalCount)
				+ " records.");
		LOG.info("DHBP : " + "Got CSV data.");
		return licLoaEccnData;
	}

	private void writeLicLoaEccnData(
			List<HashMap<String, String>> licLoaEccnData) {
		String licLoaEccnDataFilePath = PluginConstants.installLocation
				+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
				+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER
				+ PluginConstants.loaEccnFileName;
		LOG.info("DHBP : " + "Writing LIC-LOA-ECCN data to file - "
				+ licLoaEccnDataFilePath);
		writeSerializableDataToFile(licLoaEccnData, licLoaEccnDataFilePath);
		LOG.info("DHBP : " + "Wrote data to file.");
	}

	private byte[] zipFiles(List<String> files) {
		LOG.info("DHBP : " + "Creating zip file.");
		String zipFile = PluginConstants.installLocation
				+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
				+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER
				+ PluginConstants.compressedFileName;
		final List<String> zipEntries = files;
		Utils.compressFiles(zipEntries, zipFile);
		byte[] bytes = null;
		try {
			final File file = new File(zipFile);
			final InputStream is = new FileInputStream(zipFile);
			int offset = 0;
			int numRead = 0;
			bytes = new byte[(int) file.length()];
			while (offset < bytes.length
					&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
				offset += numRead;
			}
			is.close();
			file.delete();
		} catch (IOException e) {
			LOG.error("DHBP : " + "IO Exception in creating zip file."
					+ e.getMessage());
		}
		return bytes;
	}

	/** Prepares the data to be sent to the Policy Controller based on the
	 * request recieved.
	 * 
	 * @param request Contains the request from the Policy Controller.
	 * @return HashMap containing data to be updated on the client side. */
	private Serializable prepareData(Map<String, Object> request) {
		Date pcRequestTime = null;
		if (request.containsKey("Date")) {
			pcRequestTime = new Timestamp(
					((Date) request.get("Date")).getTime());
		} else {
			pcRequestTime = new Timestamp(0);
			LOG.error("DHBP : " + "PC request time is NULL or empty.");
		}
		String licLoaEccnFile = PluginConstants.pluginProps
				.getProperty("csv_file_path");
		LOG.info("DHBP : " + "LOA ECCN CSV File Path : " + licLoaEccnFile);
		Date loaDbUpdateTime = null;
		if (licLoaEccnFile != null && !licLoaEccnFile.isEmpty()) {
			if (new File(licLoaEccnFile).exists()) {
				loaDbUpdateTime = new Timestamp(
						new File(licLoaEccnFile).lastModified());
			} else {
				loaDbUpdateTime = new Timestamp(0);
				LOG.error("DHBP : "
						+ "LIC-LOA-CSV file modified time doesn't exist.");
			}
		} else {
			loaDbUpdateTime = new Timestamp(0);
			LOG.error("DHBP : "
					+ "Error in getting LIC-LOA-CSV file modified time.");
		}
		Date dictUpdateTime = null;
		dictUpdateTime = DictionaryData.getLastModifiedDate();
		if (dictUpdateTime == null) {
			dictUpdateTime = new Timestamp(0);
			LOG.error("DHBP : " + "Error in getting Dictionary DB update time.");
		}
		String dictDataFilePath = PluginConstants.installLocation
				+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
				+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER
				+ PluginConstants.dictDataFileName;
		String loaEccnFilePath = PluginConstants.installLocation
				+ PluginConstants.CCROOT + PluginConstants.JARFOLDER
				+ PluginConstants.APPFOLDER + PluginConstants.DATAFOLDER
				+ PluginConstants.loaEccnFileName;
		Date loaEccnModifiedTime = null;
		Date dictDataModifiedTime = null;
		if (new File(dictDataFilePath).exists()) {
			dictDataModifiedTime = new Timestamp(
					new File(dictDataFilePath).lastModified());
		} else {
			dictDataModifiedTime = new Timestamp(0);
		}
		if (new File(loaEccnFilePath).exists()) {
			loaEccnModifiedTime = new Timestamp(
					new File(loaEccnFilePath).lastModified());
		} else {
			loaEccnModifiedTime = new Timestamp(0);
		}
		LOG.info("DHBP : " + "The Policy Controller request time is     : "
				+ pcRequestTime.toString());
		LOG.info("DHBP : " + "The loa-eccn DB last update time is       : "
				+ loaDbUpdateTime.toString());
		LOG.info("DHBP : " + "The dictionary DB last update time is     : "
				+ dictUpdateTime.toString());
		LOG.info("DHBP : " + "The last modified time for dict data file : "
				+ dictDataModifiedTime.toString());
		LOG.info("DHBP : " + "The last modified time for loa eccn file  : "
				+ loaEccnModifiedTime.toString());
		boolean returnDictFile = false;
		boolean returnLoaFile = false;
		boolean fetchDictData = false;
		boolean fetchLoaData = false;
		LOG.info("DHBP : " + "Making decision for LOA ECCN CSV...");
		final decision loaDecision = makeDecision(pcRequestTime,
				loaEccnModifiedTime, loaDbUpdateTime);
		if (loaDecision == decision.SENDFILE) {
			LOG.info("DHBP : " + "Decision for LOA ECCN CSV is SEND FILE.");
			returnLoaFile = true;
		} else if (loaDecision == decision.UPDATEFILEANDSEND) {
			LOG.info("DHBP : "
					+ "Decision for LOA ECCN CSV is UPDATE FILE  AND SEND FILE.");
			fetchLoaData = true;
			returnLoaFile = true;
		} else {
			LOG.info("DHBP : " + "Decision for LOA ECCN CSV is NONE.");
		}
		LOG.info("DHBP : " + "Making decision for Dictionary DB...");
		final decision dictDecision = makeDecision(pcRequestTime,
				dictDataModifiedTime, dictUpdateTime);
		if (dictDecision == decision.SENDFILE) {
			LOG.info("DHBP : " + "Decision for Dictionary DB is SEND FILE.");
			returnDictFile = true;
		} else if (dictDecision == decision.UPDATEFILEANDSEND) {
			LOG.info("DHBP : "
					+ "Decision for Dictionary DB is UPDATE FILE  AND SEND FILE.");
			fetchDictData = true;
			returnDictFile = true;
		} else {
			LOG.info("DHBP : " + "Decision for Dictionary DB is NONE.");
		}
		LOG.info("DHBP : " + "Checking if files exist.");
		if (!(new File(dictDataFilePath).exists())) {
			LOG.info("DHBP : " + "Dict data file missing. Creating file.");
			fetchDictData = true;
		}
		if (!(new File(loaEccnFilePath).exists())) {
			LOG.info("DHBP : " + "LOA-ECCN data file missing. Creating file.");
			fetchLoaData = true;
		}
		if (!fetchLoaData && !returnLoaFile && !fetchDictData
				&& !returnDictFile) {
			LOG.info("DHBP : " + "Nothing to return.");
			return null;
		}
		if (fetchDictData) {
			List<HashMap<String, String>> dictionaryData = getDictionaryData();
			if (dictionaryData != null && !dictionaryData.isEmpty()) {
				writeDictionaryData(dictionaryData);
			}
		}
		if (fetchLoaData) {
			List<HashMap<String, String>> licLoaEccnData = getLicLoaEccnData();
			if (licLoaEccnData != null && !licLoaEccnData.isEmpty()) {
				writeLicLoaEccnData(licLoaEccnData);
			}
		}
		List<String> files = new ArrayList<String>();
		if (returnDictFile) {
			if (new File(dictDataFilePath).exists()) {
				files.add(dictDataFilePath);
			} else {
				List<HashMap<String, String>> dictionaryData = getDictionaryData();
				if (dictionaryData != null && !dictionaryData.isEmpty()) {
					writeDictionaryData(dictionaryData);
				}
				if (new File(dictDataFilePath).exists()) {
					files.add(dictDataFilePath);
				} else {
					LOG.error("DHBP : "
							+ "Unable to create Dictionary Data file.");
				}
			}
		}
		if (returnLoaFile) {
			if (new File(loaEccnFilePath).exists()) {
				files.add(loaEccnFilePath);
			} else {
				List<HashMap<String, String>> licLoaEccnData = getLicLoaEccnData();
				if (licLoaEccnData != null && !licLoaEccnData.isEmpty()) {
					writeLicLoaEccnData(licLoaEccnData);
				}
				if (new File(loaEccnFilePath).exists()) {
					files.add(loaEccnFilePath);
				} else {
					LOG.error("DHBP : "
							+ "Unable to create LIA-LOA-ECCN Data file.");
				}
			}
		}
		HashMap<String, Object> result = new HashMap<String, Object>();
		if (returnDictFile) {
			result.put("dictFile", "YES");
		} else {
			result.put("dictFile", "NO");
		}
		if (returnLoaFile) {
			result.put("loaFile", "YES");
		} else {
			result.put("loaFile", "NO");
		}
		byte[] zipData = zipFiles(files);
		if (zipData != null) {
			result.put("Data", zipData);
		} else {
			result.put("Data", null);
		}
		LOG.info("DHBP : " + "Returning : " + result.toString());
		return result;
	}

	/** Services the heartbeat request from the client and returns the updated
	 * data, if any, from the server.
	 * 
	 * @param name name of the heartbeat plugin.
	 * @param data the data sent by the client (HashMap containing pc request
	 * date)
	 * @return HashMap containing data. */
	public Serializable serviceHeartbeatRequest(String name, Serializable data) {
		if (!name.equals(PluginConstants.NAME))
			return null;
		final HashMap<String, Object> request = (HashMap<String, Object>) data;
		LOG.info("DHBP : "
				+ "DowHeartbeatPlugin - Recieved request from client - "
				+ request);
		try {
			return prepareData(request);
		} catch (Exception e) {
			LOG.error("DHBP : " + "Exception : ", e);
			return null;
		}
	}

	/** Writes any serializable object to file.
	 * 
	 * @param o object to be written to file.
	 * @param path path at which file has to be created. */
	private void writeSerializableDataToFile(Object o, String path) {
		try {
			final OutputStream file = new FileOutputStream(path);
			final OutputStream buffer = new BufferedOutputStream(file);
			final ObjectOutput output = new ObjectOutputStream(buffer);
			try {
				output.writeObject(o);
			} finally {
				output.close();
			}
		} catch (IOException ex) {
			LOG.error("DHBP : " + "Cannot write to file.", ex);
		}
	}

	public static void main(String[] args) {
		String licLoaEccnFile = "C:\\Users\\pbalaji\\Desktop\\licloaeccn.csv";
		BufferedReader br = null;
		String line = "";
		String csvSplitBy = ",";
		List<List<String>> result = new ArrayList<List<String>>();
		boolean isHeading = true;
		try {
			br = new BufferedReader(new FileReader(licLoaEccnFile));
			while ((line = br.readLine()) != null) {
				if (isHeading) {
					isHeading = false;
					continue;
				}
				String[] items = line.split(csvSplitBy);
				String loa = items[0];
				String license = items[1];
				String eccn = items[2];
				String effectiveDate = items[3];
				String expiryDate = items[4];
				if (loa == null || loa.isEmpty()) {
					loa = "NULL";
				}
				if (license == null || license.isEmpty()) {
					license = "NULL";
				}
				DateFormat sourceDf = new SimpleDateFormat("MM/dd/yyyy");
				DateFormat targetDf = new SimpleDateFormat("yyyy-MM-dd");
				try {
					effectiveDate = targetDf.format(sourceDf
							.parse(effectiveDate));
					expiryDate = targetDf.format(sourceDf.parse(expiryDate));
				} catch (ParseException e) {
					System.out
							.println("Parsing Error : Date not in expected format.");
				}
				List<String> csvLine = new ArrayList<String>();
				csvLine.add(loa);
				csvLine.add(license);
				csvLine.add(eccn);
				csvLine.add(effectiveDate);
				csvLine.add(expiryDate);
				result.add(csvLine);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println(result.toString());
	}

	/** New HeartBeat Implementation Interface from CC 6.5 */
	public Serializable serviceHeartbeatRequest(String name, String data) {
		LOG.info("input parameter name " + name);
		// log.info("input parameter data:" + data);
		LOG.info("input parameter data length:" + data.length());
		if (!name.equals(PluginConstants.NAME)) {
			return null;
		}
		HashMap localHashMap = new HashMap();
		localHashMap = (HashMap) SerializationUtils.unwrapSerialized(data);
		LOG.info("input request:" + localHashMap);
		return prepareData(localHashMap);
	}
}