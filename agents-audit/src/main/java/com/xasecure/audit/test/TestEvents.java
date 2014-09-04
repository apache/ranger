package com.xasecure.audit.test;
import org.apache.commons.logging.Log;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;

import com.xasecure.audit.model.AuditEventBase;
import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.model.HdfsAuditEvent;
import com.xasecure.audit.model.HiveAuditEvent;
import com.xasecure.audit.model.KnoxAuditEvent;
import com.xasecure.audit.model.StormAuditEvent;
import com.xasecure.audit.provider.AuditProvider;
import com.xasecure.audit.provider.AuditProviderFactory;

public class TestEvents {

	private static final Log LOG = LogFactory.getLog(TestEvents.class);

    public static void main(String[] args) {
    	DOMConfigurator.configure("log4j.xml");

        LOG.info("==> TestEvents.main()");
        
        try {
        	Properties auditProperties = new Properties();
        	
        	String AUDIT_PROPERTIES_FILE = "xasecure-audit.properties";
        	
        	File propFile = new File(AUDIT_PROPERTIES_FILE);
        	
        	if(propFile.exists()) {
            	LOG.info("Loading Audit properties file" + AUDIT_PROPERTIES_FILE);

            	auditProperties.load(new FileInputStream(propFile));
        	} else {
            	LOG.info("Audit properties file missing: " + AUDIT_PROPERTIES_FILE);

            	auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.url", "jdbc:mysql://localhost:3306/xa_db");
	        	auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.user", "xaaudit");
	        	auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.password", "xaaudit");
	        	auditProperties.setProperty("xasecure.audit.jpa.javax.persistence.jdbc.driver", "com.mysql.jdbc.Driver");
	
	        	auditProperties.setProperty("xasecure.audit.is.enabled", "true");
	        	auditProperties.setProperty("xasecure.audit.log4j.is.enabled", "false");
	        	auditProperties.setProperty("xasecure.audit.log4j.is.async", "false");
	        	auditProperties.setProperty("xasecure.audit.log4j.async.max.queue.size", "100000");
	        	auditProperties.setProperty("xasecure.audit.log4j.async.max.flush.interval.ms", "30000");
	        	auditProperties.setProperty("xasecure.audit.db.is.enabled", "true");
	        	auditProperties.setProperty("xasecure.audit.db.is.async", "true");
	        	auditProperties.setProperty("xasecure.audit.db.async.max.queue.size", "100000");
	        	auditProperties.setProperty("xasecure.audit.db.async.max.flush.interval.ms", "30000");
	        	auditProperties.setProperty("xasecure.audit.db.batch.size", "100");
        	}
        	
        	AuditProviderFactory.getInstance().init(auditProperties);

        	AuditProvider provider = AuditProviderFactory.getAuditProvider();

        	LOG.info("provider=" + provider.toString());

        	String strEventCount = args.length > 0 ? args[0] : auditProperties.getProperty("xasecure.audit.test.event.count");

        	int eventCount = (strEventCount == null) ? 1024 : Integer.parseInt(strEventCount);

        	for(int i = 0; i < eventCount; i++) {
        		AuditEventBase event = getTestEvent(i);

	            LOG.info("==> TestEvents.main(" + (i+1) + "): adding " + event.getClass().getName());
        		provider.log(event);

                if(i != 0 && ((i % 100) == 0))
                    Thread.sleep(100);
        	}
        } catch(Exception excp) {
            LOG.info(excp.getLocalizedMessage());
        	excp.printStackTrace();
        }

        LOG.info("<== TestEvents.main()");
    }
    
    private static AuditEventBase getTestEvent(int idx) {
    	AuditEventBase event = null;
 
		switch(idx % 5) {
			case 0:
				event = new HdfsAuditEvent();
			break;
			case 1:
				event = new HBaseAuditEvent();
			break;
			case 2:
				event = new HiveAuditEvent();
			break;
			case 3:
				event = new KnoxAuditEvent();
			break;
			case 4:
				event = new StormAuditEvent();
			break;
		}
		event.setEventTime(new Date());
		event.setResultReason(Integer.toString(idx));

		return event;
    }
}
