package com.nextlabs.hb.helper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.dictionary.Dictionary;
import com.bluejungle.dictionary.DictionaryException;
import com.bluejungle.dictionary.ElementFieldData;
import com.bluejungle.dictionary.IDictionaryIterator;
import com.bluejungle.dictionary.IElementField;
import com.bluejungle.dictionary.IElementType;
import com.bluejungle.dictionary.IEnrollment;
import com.bluejungle.framework.comp.ComponentManagerFactory;
import com.bluejungle.framework.expressions.IPredicate;

public class DictionaryData {

	private static final Log LOG = LogFactory.getLog(DictionaryData.class);

	public Date getLatestConsistentTime() {

		try {
			Dictionary dict = ComponentManagerFactory.getComponentManager()
					.getComponent(Dictionary.COMP_INFO);
			return dict.getLatestConsistentTime();
		} catch (DictionaryException e) {
			LOG.error("Error getting latest consistent time.", e);
			return null;
		}

	}

	public static List<HashMap<String, String>> getUserData() {

		List<HashMap<String, String>> userData = null;
		IDictionaryIterator<ElementFieldData> efdList = null;

		try {

			LOG.debug("Accessing Dictionary Database.");

			Dictionary dict = ComponentManagerFactory.getComponentManager()
					.getComponent(Dictionary.COMP_INFO);
			Collection<IEnrollment> enrollments = dict.getEnrollments();
			userData = new ArrayList<HashMap<String, String>>();

			for (IEnrollment enroll : enrollments) {

				if (enroll.getIsActive()) {

					Date ct = dict.getLatestConsistentTime();

					LOG.info("Latest dictionary consistent time - "
							+ dict.getLatestConsistentTime().toString());

					IElementType eType = dict.getType("USER");
					IPredicate ipred = dict.condition(eType);

					String[] sProperties = enroll.getExternalNames(eType);

					int spropertylength = sProperties.length;

					ipred = dict.condition(enroll);

					LOG.debug("Querying enrollment.");

					IElementField[] iefArray = new IElementField[spropertylength];

					for (int i = 0; i < spropertylength; i++) {

						iefArray[i] = enroll.lookupField(eType, sProperties[i])[0];

					}

					efdList = dict.queryFields(iefArray, ipred, ct, null, null);

					if (efdList != null) {

						while (efdList.hasNext()) {

							ElementFieldData efd = efdList.next();

							HashMap<String, String> record = new HashMap<String, String>();
							
							record.put("Enrollment", enroll.getDomainName());

							Object[] data = efd.getData();

							for (int i = 0; i < spropertylength; i++) {

								if (data[i] != null) {

									if (!(data[i].getClass().isArray())) {

										record.put(sProperties[i],

										data[i].toString());

									} else {

										String separator = PluginConstants.commonProps

										.getProperty(sProperties[i]

										+ "_separator");

										if (separator != null) {

											StringBuffer value = new StringBuffer();

											Object[] datas = (Object[]) data[i];

											for (Object st : datas) {

												value.append(st.toString());

												value.append(separator);

											}

											record.put(sProperties[i],

											value.toString());

										} else {

											Object[] datas = (Object[]) data[i];

											record.put(sProperties[i],

											datas[0].toString());

										}

									}

								}

							}

							userData.add(record);

						}

					}

					else {

						LOG.info("User list is NULL.");

					}

				}

			}

		} catch (Exception ex) {

			LOG.error("Exception in getUserData - ", ex);

		} finally {
			if (efdList != null)
				try {
					efdList.close();
				} catch (DictionaryException e) {
					LOG.error("Dictionary Exception while closing iterator. ",
							e);
				}
			LOG.debug("Closed iterator.");
		}

		return userData;

	}

	public static Timestamp getLastModifiedDate() {

		Timestamp ct = null;

		try {

			Dictionary dict = ComponentManagerFactory.getComponentManager()

			.getComponent(Dictionary.COMP_INFO);

			// Getting all the enrollments

			Collection<IEnrollment> enrollments = dict.getEnrollments();

			// Loop through the user enrollment data

			for (IEnrollment enroll : enrollments) {

				// Only getting Active enrollment

				if (enroll.getIsActive()) {

					ct = new Timestamp(dict.getLatestConsistentTime().getTime());

				}

			}

		} catch (DictionaryException e) {

			LOG.error("Dictionary Data getLastModifiedDate()" + e.toString(), e);

		}

		return ct;

	}

}
