package com.mycompany.simple_flink;

/**
 * POJO for aggregation key.
 * <p><b>Important!</b></p>
 * Criteria for a POJO to serve as key
 * <ol>
 * <li>Must implement hashCode()</li>
 * <li>Must have a zero-parameter, public constructor</li>
 * <li>Fields must be accesible: public or with getter and setter</li>
 * </ol>
 */
public class AggregationKey {
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((msisdn == null) ? 0 : msisdn.hashCode());
		result = prime * result + ((pointTarget == null) ? 0 : pointTarget.hashCode());
		result = prime * result + ((usageType == null) ? 0 : usageType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AggregationKey other = (AggregationKey) obj;
		if (msisdn == null) {
			if (other.msisdn != null)
				return false;
		} else if (!msisdn.equals(other.msisdn))
			return false;
		if (pointTarget == null) {
			if (other.pointTarget != null)
				return false;
		} else if (!pointTarget.equals(other.pointTarget))
			return false;
		if (usageType == null) {
			if (other.usageType != null)
				return false;
		} else if (!usageType.equals(other.usageType))
			return false;
		return true;
	}

	private String msisdn;
	private String usageType;
	private String pointTarget;
	
	public AggregationKey(){
		super();
	}

	public AggregationKey(String msisdn, String usageType, String pointTarget) {
		super();
		this.msisdn = msisdn;
		this.usageType = usageType;
		this.pointTarget = pointTarget;
	}

	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public String getUsageType() {
		return usageType;
	}

	public void setUsageType(String usageType) {
		this.usageType = usageType;
	}

	public String getPointTarget() {
		return pointTarget;
	}

	public void setPointTarget(String pointTarget) {
		this.pointTarget = pointTarget;
	}

}
