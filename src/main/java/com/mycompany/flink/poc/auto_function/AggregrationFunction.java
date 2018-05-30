package com.mycompany.flink.poc.auto_function;


import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

public class AggregrationFunction {
	
	private static Logger logger = Logger.getLogger(AggregrationFunction.class);

	public JSONObject reduce(JSONObject r1, JSONObject r2) {
		try {
			int sum = r1.getInt("I_PRIMARY_UNITS") + r2.getInt("I_PRIMARY_UNITS");
			//set into r1
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return r1;
	}
}