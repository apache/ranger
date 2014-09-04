package com.xasecure.pdp.storm;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.storm.XaStormAccessVerifier;
import com.xasecure.pdp.config.PolicyChangeListener;
import com.xasecure.pdp.config.PolicyRefresher;
import com.xasecure.pdp.constants.XaSecureConstants;
import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.RolePermission;


public class URLBasedAuthDB implements PolicyChangeListener, XaStormAccessVerifier {
	
	private static final Logger LOG = LoggerFactory.getLogger(URLBasedAuthDB.class) ;

	private static URLBasedAuthDB me = null;
	
	private PolicyRefresher refresher = null ;
	
	private PolicyContainer policyContainer = null;
	
	private List<StormAuthRule> stormAuthDB = null ; 
	
	public static URLBasedAuthDB getInstance() {
		if (me == null) {
			synchronized (URLBasedAuthDB.class) {
				URLBasedAuthDB temp = me;
				if (temp == null) {
					me = new URLBasedAuthDB();
					me.init() ;
				}
			}
		}
		return me;
	}
	
	private URLBasedAuthDB() {
		
		String url 			 = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_STORM_POLICYMGR_URL_PROP);
		
		long  refreshInMilli = XaSecureConfiguration.getInstance().getLong(
				XaSecureConstants.XASECURE_STORM_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP ,
				XaSecureConstants.XASECURE_STORM_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT);
		
		String lastStoredFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_STORM_LAST_SAVED_POLICY_FILE_PROP) ;
		
		String sslConfigFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_STORM_POLICYMGR_SSL_CONFIG_FILE_PROP) ;
		
		refresher = new PolicyRefresher(url, refreshInMilli,sslConfigFileName,lastStoredFileName) ;
		
		String saveAsFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_STORM_POLICYMGR_URL_SAVE_FILE_PROP) ;
		if (saveAsFileName != null) {
			refresher.setSaveAsFileName(saveAsFileName) ;
		}
		
		if (lastStoredFileName != null) {
			refresher.setLastStoredFileName(lastStoredFileName);
		}	
	}
	
	
	private void init() {
		refresher.setPolicyChangeListener(this);
	}
	
	
	@Override
	public void OnPolicyChange(PolicyContainer aPolicyContainer) {
		setPolicyContainer(aPolicyContainer);
	}
	
	
	public PolicyContainer getPolicyContainer() {
		return policyContainer;
	}

	
	
	public synchronized void setPolicyContainer(PolicyContainer aPolicyContainer) {
		
		if (aPolicyContainer != null) {
			
			List<StormAuthRule> tempStormAuthDB = new ArrayList<StormAuthRule>() ;
			
			for(Policy p : aPolicyContainer.getAcl()) {
				
				if (! p.isEnabled()) {
					continue;
				}
				
				for (String topologyName : p.getTopologyList()) {
					
					List<RolePermission> rpList = p.getPermissions() ;
					
					for(RolePermission rp : rpList) {
						StormAuthRule rule = new StormAuthRule(topologyName, rp.getAccess() , rp.getUsers(), rp.getGroups(), (p.getAuditInd() == 1)) ;
						tempStormAuthDB.add(rule) ;
					}
				}
			}
			
			this.stormAuthDB = tempStormAuthDB ;
			
			this.policyContainer = aPolicyContainer ;
		}
	}

	@Override
	public boolean isAccessAllowed(String aUserName, String[] aGroupName, String aOperationName, String aTopologyName) {

		boolean accessAllowed = false ;

		List<StormAuthRule> tempStormAuthDB =  this.stormAuthDB ;
		
		if (tempStormAuthDB != null) {
			for(StormAuthRule rule : tempStormAuthDB) {
				if (rule.isMatchedTopology(aTopologyName)) {
					if (rule.isOperationAllowed(aOperationName)) {
						if (rule.isUserAllowed(aUserName, aGroupName)) {
							accessAllowed = true ;
							break ;
						}
					}
				}
			}
		}
		
		return accessAllowed ;
	}

	@Override
	public boolean isAudited(String aTopologyName) {
		boolean auditEnabled = false ;

		List<StormAuthRule> tempStormAuthDB =  stormAuthDB ;
		
		if (tempStormAuthDB != null) {
			for(StormAuthRule rule : tempStormAuthDB) {
				if (rule.isMatchedTopology(aTopologyName)) {
					auditEnabled = rule.getAuditEnabled() ;
					if (auditEnabled) {
						break ;
					}
				}
			}
		}
		
		return auditEnabled ;
	}
	
}