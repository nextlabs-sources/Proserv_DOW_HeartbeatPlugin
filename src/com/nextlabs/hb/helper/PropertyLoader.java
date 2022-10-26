package com.nextlabs.hb.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PropertyLoader {

	private static final Log LOG = LogFactory.getLog(PropertyLoader.class);

	public static Properties loadProperties(final String name) {

		if (name == null) {
			throw new IllegalArgumentException("Null property file name.");
		}

		String pathName = null;

		if (name.charAt(0) == '/') {
			pathName = "." + name;
		} else {
			pathName = name;
		}

		Properties result = null;
		try {
			final File file = new File(pathName);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Properties File Path - " + file.getAbsolutePath());
			}

			if (file != null) {
				final FileInputStream fis = new FileInputStream(file);
				result = new Properties();
				result.load(fis);
			}
		} catch (IOException e) {
			LOG.error("Error parsing properties file - ", e);
		}
		return result;
	}

}
