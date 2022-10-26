package com.nextlabs.hb.helper;

import java.util.Properties;

public class PluginConstants {

	public static final String NAME = " DowHeartbeatPlugin";

	public static String installLocation = "";

	public static final String PCROOT = "/jservice";

	public static final String CCROOT = "/server/plugins";
	
	public static final String LOGROOT = "/server/logs";

	public static final String CONFFOLDER = "/config";

	public static final String APPFOLDER = "/dow";

	public static final String JARFOLDER = "/jar";

	public static final String DATAFOLDER = "/data/";

	public static final String PCINMEMDB = "DowLicenseDB";

	public static Properties commonProps;

	public static Properties pluginProps;

	public static String dictDataFileName = "dictdata.bin";

	public static String loaEccnFileName = "loaeccndata.bin";

	public static String compressedFileName = "data.zip";	 
	
	public static String loaEccnCsvErrorLog = "licloaeccnerrors.log";
	
	public static String dictDataErrorLog = "dictdataerrors.log";

}
