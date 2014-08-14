#!/bin/sh

USAGE="Usage: setup_authentication.sh [UNIX|LDAP|AD|NONE] <path>"

if [ $# -ne 2 ]
  then
    echo $USAGE;
fi

authentication_method=$1
path=$2

if [ $authentication_method = "UNIX" ] ; then
    	echo $path;
	awk 'FNR==NR{ _[++d]=$0;next}
	/UNIX_BEAN_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/UNIX_BEAN_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/unix_bean_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml
	
	awk 'FNR==NR{ _[++d]=$0;next}
	/UNIX_SEC_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/UNIX_SEC_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/unix_security_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml

    exit 0;

elif [ $authentication_method = "LDAP" ]; then
	echo $path;
	awk 'FNR==NR{ _[++d]=$0;next}
	/LDAP_BEAN_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/LDAP_BEAN_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/ldap_bean_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml
		
	awk 'FNR==NR{ _[++d]=$0;next}
	/LDAP_SEC_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/LDAP_SEC_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/ldap_security_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml

    exit 0;
			
elif [ $authentication_method = "ACTIVE_DIRECTORY" ]; then
	 echo $path;
	    awk 'FNR==NR{ _[++d]=$0;next}
	/AD_BEAN_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/AD_BEAN_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/ad_bean_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml
		
	awk 'FNR==NR{ _[++d]=$0;next}
	/AD_SEC_SETTINGS_START/{
	  print
	  for(i=1;i<=d;i++){ print _[i] }
	  f=1;next
	}
	/AD_SEC_SETTINGS_END/{f=0}!f' $path/META-INF/contextXML/ad_security_settings.xml $path/META-INF/security-applicationContext.xml  > tmp
	mv tmp $path/META-INF/security-applicationContext.xml

    exit 0;
elif [ $authentication_method = "NONE" ]; then
echo $path;
    exit 0;
else
    echo $USAGE;
    exit 1;
fi




