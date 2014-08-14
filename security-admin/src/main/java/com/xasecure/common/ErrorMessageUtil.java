/**
 *
 */
package com.xasecure.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class ErrorMessageUtil extends PropertyPlaceholderConfigurer {
    private static Map<String, String> messageMap;

    private ErrorMessageUtil() {

    }

    @Override
    protected void processProperties(
	    ConfigurableListableBeanFactory beanFactory, Properties props)
	    throws BeansException {
	super.processProperties(beanFactory, props);

	messageMap = new HashMap<String, String>();
	Set<Object> keySet = props.keySet();

	for (Object key : keySet) {
	    String keyStr = key.toString();
	    messageMap.put(keyStr, props.getProperty(keyStr));
	}
    }


    public static String getMessage(String key) {
	return messageMap.get(key);
    }

}