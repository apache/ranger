/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.starrocks.client;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.security.auth.Subject;

public class StarRocksClient extends BaseClient implements Closeable {
    public static final String STARROCKS_USER_NAME_PROP = "user";
    public static final String STARROCKS_PASSWORD_PROP = "password";

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksClient.class);

    private static final String ERR_MSG = "You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check ranger_admin.log for more info.";

    private Connection con;

    public StarRocksClient(String serviceName) throws Exception {
        super(serviceName, null);
        init();
    }

    public StarRocksClient(String serviceName, Map<String, String> properties) throws Exception {
        super(serviceName, properties);
        init();
    }

    private void init() throws Exception {
        Subject.doAs(getLoginSubject(), new PrivilegedAction<Void>() {
            public Void run() {
                initConnection();
                return null;
            }
        });
    }

    private void initConnection() {
        Properties prop = getConfigHolder().getRangerSection();
        String driverClassName = prop.getProperty("jdbc.driverClassName");
        String url = prop.getProperty("jdbc.url");

        Properties starrocksProperties = new Properties();
        String decryptedPwd = null;
        try {
            decryptedPwd = PasswordUtils.decryptPassword(getConfigHolder().getPassword());
        } catch (Exception ex) {
            LOG.info("Password decryption failed");
            decryptedPwd = null;
        } finally {
            if (decryptedPwd == null) {
                decryptedPwd = prop.getProperty(HadoopConfigHolder.RANGER_LOGIN_PASSWORD);
            }
        }
        starrocksProperties.put(STARROCKS_USER_NAME_PROP, prop.getProperty(HadoopConfigHolder.RANGER_LOGIN_USER_NAME_PROP));
        if (prop.getProperty(HadoopConfigHolder.RANGER_LOGIN_PASSWORD) != null) {
            starrocksProperties.put(STARROCKS_PASSWORD_PROP, decryptedPwd);
        }

        if (driverClassName != null) {
            try {
                Driver driver = (Driver) Class.forName(driverClassName).newInstance();
                DriverManager.registerDriver(driver);
            } catch (SQLException e) {
                String msgDesc = "initConnection: Caught SQLException while registering"
                        + " the StarRocks driver.";
                HadoopException hdpException = new HadoopException(msgDesc, e);
                hdpException.generateResponseDataMap(false, getMessage(e),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            } catch (IllegalAccessException ilae) {
                String msgDesc = "initConnection: Class or its nullary constructor might not accessible.";
                HadoopException hdpException = new HadoopException(msgDesc, ilae);
                hdpException.generateResponseDataMap(false, getMessage(ilae),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            } catch (InstantiationException ie) {
                String msgDesc = "initConnection: Class may not have its nullary constructor or "
                        + "may be the instantiation fails for some other reason.";
                HadoopException hdpException = new HadoopException(msgDesc, ie);
                hdpException.generateResponseDataMap(false, getMessage(ie),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            } catch (ExceptionInInitializerError eie) {
                String msgDesc = "initConnection: Got ExceptionInInitializerError, "
                        + "The initialization provoked by this method fails.";
                HadoopException hdpException = new HadoopException(msgDesc,
                        eie);
                hdpException.generateResponseDataMap(false, getMessage(eie),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            } catch (SecurityException se) {
                String msgDesc = "initConnection: unable to initiate connection to StarRocks instance,"
                        + " The caller's class loader is not the same as or an ancestor "
                        + "of the class loader for the current class and invocation of "
                        + "s.checkPackageAccess() denies access to the package of this class.";
                HadoopException hdpException = new HadoopException(msgDesc, se);
                hdpException.generateResponseDataMap(false, getMessage(se),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            } catch (Throwable t) {
                String msgDesc = "initConnection: Unable to connect to StarRocks instance, "
                        + "please provide valid value of field : {jdbc.driverClassName}.";
                HadoopException hdpException = new HadoopException(msgDesc, t);
                hdpException.generateResponseDataMap(false, getMessage(t),
                        msgDesc + ERR_MSG, null, null);
                throw hdpException;
            }
        }

        try {
            con = DriverManager.getConnection(url, starrocksProperties);
        } catch (SQLException e) {
            String msgDesc = "Unable to connect to StarRocks instance.";
            HadoopException hdpException = new HadoopException(msgDesc, e);
            hdpException.generateResponseDataMap(false, getMessage(e),
                    msgDesc + ERR_MSG, null, null);
            throw hdpException;
        } catch (SecurityException se) {
            String msgDesc = "Unable to connect to StarRocks instance.";
            HadoopException hdpException = new HadoopException(msgDesc, se);
            hdpException.generateResponseDataMap(false, getMessage(se),
                    msgDesc + ERR_MSG, null, null);
            throw hdpException;
        } catch (Throwable t) {
            String msgDesc = "initConnection: Unable to connect to StarRocks instance, ";
            HadoopException hdpException = new HadoopException(msgDesc, t);
            hdpException.generateResponseDataMap(false, getMessage(t),
                    msgDesc + ERR_MSG, null, null);
            throw hdpException;
        }

    }

    private List<String> getCatalogs(String needle, List<String> catalogs) throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = "SHOW CATALOGS";

            try {
                if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                    // Cannot use a prepared statement for this as starrocks does not support that
                    sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                }
                stat = con.createStatement();
                rs = stat.executeQuery(sql);
                while (rs.next()) {
                    String catalogName = rs.getString(1);
                    if (catalogs != null && catalogs.contains(catalogName)) {
                        continue;
                    }
                    ret.add(catalogName);
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
            } catch (SQLException se) {
                String msg = "Unable to execute SQL [" + sql + "]. ";
                HadoopException he = new HadoopException(msg, se);
                he.generateResponseDataMap(false, getMessage(se), msg + ERR_MSG,
                        null, null);
                throw he;
            } finally {
                close(rs);
                close(stat);
            }
        }
        return ret;
    }

    public List<String> getCatalogList(String needle, final List<String> catalogs) throws HadoopException {
        final String ndl = needle;
        final List<String> catList = catalogs;

        List<String> dbs = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                List<String> ret = null;
                try {
                    ret = getCatalogs(ndl, catList);
                } catch (HadoopException he) {
                    LOG.error("<== StarRocksClient.getCatalogList() :Unable to get the Database List", he);
                    throw he;
                }
                return ret;
            }
        });

        return dbs;
    }

    private List<String> getDatabases(String needle, List<String> catalogs, List<String> schemas) throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                if (catalogs != null && !catalogs.isEmpty()) {
                    for (String catalog : catalogs) {
                        sql = "SHOW DATABASES FROM `" + StringEscapeUtils.escapeSql(catalog) + "`";

                        try {
                            if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                                sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                            }
                            stat = con.createStatement();
                            rs = stat.executeQuery(sql);
                            while (rs.next()) {
                                String schema = rs.getString(1);
                                if (schemas != null && schemas.contains(schema)) {
                                    continue;
                                }
                                ret.add(schema);
                            }
                        } finally {
                            close(rs);
                            close(stat);
                            rs = null;
                            stat = null;
                        }
                    }
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getSchemas() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getSchemas() Error : ", sqle);
                }
                throw hdpException;
            }
        }

        return ret;
    }

    public List<String> getDatabaseList(String needle, List<String> catalogs, List<String> schemas) throws HadoopException {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;

        List<String> schemaList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                List<String> ret = null;
                try {
                    ret = getDatabases(ndl, cats, shms);
                } catch (HadoopException he) {
                    LOG.error("<== StarRocksClient.getSchemaList() :Unable to get the Schema List", he);
                }
                return ret;
            }
        });

        return schemaList;
    }

    private List<String> getTables(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            if (catalogs != null && !catalogs.isEmpty()
                    && schemas != null && !schemas.isEmpty()) {
                try {
                    for (String catalog : catalogs) {
                        for (String schema : schemas) {
                            sql = "SHOW tables FROM `" + StringEscapeUtils.escapeSql(catalog) + "`.`" +
                                    StringEscapeUtils.escapeSql(schema) + "`";
                            try {
                                if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                                    sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                                }
                                stat = con.createStatement();
                                rs = stat.executeQuery(sql);
                                while (rs.next()) {
                                    String table = rs.getString(1);
                                    if (tables != null && tables.contains(table)) {
                                        continue;
                                    }
                                    ret.add(table);
                                }
                            } finally {
                                close(rs);
                                close(stat);
                                rs = null;
                                stat = null;
                            }
                        }
                    }
                } catch (SQLTimeoutException sqlt) {
                    String msgDesc = "Time Out, Unable to execute SQL [" + sql
                            + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqlt);
                    hdpException.generateResponseDataMap(false, getMessage(sqlt),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                    }
                    throw hdpException;
                } catch (SQLException sqle) {
                    String msgDesc = "Unable to execute SQL [" + sql + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqle);
                    hdpException.generateResponseDataMap(false, getMessage(sqle),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                    }
                    throw hdpException;
                }
            }
        }
        return ret;
    }

    public List<String> getTableList(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;
        final List<String> tbls = tables;

        List<String> tableList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                List<String> ret = null;
                try {
                    ret = getTables(ndl, cats, shms, tbls);
                } catch (HadoopException he) {
                    LOG.error("<== StarRocksClient.getTableList() :Unable to get the Column List", he);
                    throw he;
                }
                return ret;
            }
        });

        return tableList;
    }

    private List<String> getColumns(String needle, List<String> catalogs, List<String> schemas, List<String> tables,
                                    List<String> columns) throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            String regex = null;
            ResultSet rs = null;
            String sql = null;
            Statement stat = null;

            if (needle != null && !needle.isEmpty()) {
                regex = needle;
            }

            if (catalogs != null && !catalogs.isEmpty()
                    && schemas != null && !schemas.isEmpty()
                    && tables != null && !tables.isEmpty()) {
                try {
                    for (String catalog : catalogs) {
                        for (String schema : schemas) {
                            for (String table : tables) {
                                sql = "SHOW COLUMNS FROM `" + StringEscapeUtils.escapeSql(catalog) + "`." +
                                        "`" + StringEscapeUtils.escapeSql(schema) + "`." +
                                        "`" + StringEscapeUtils.escapeSql(table) + "`";

                                try {
                                    stat = con.createStatement();
                                    rs = stat.executeQuery(sql);
                                    while (rs.next()) {
                                        String column = rs.getString(1);
                                        if (columns != null && columns.contains(column)) {
                                            continue;
                                        }
                                        if (regex == null) {
                                            ret.add(column);
                                        } else if (FilenameUtils.wildcardMatch(column, regex)) {
                                            ret.add(column);
                                        }
                                    }
                                } finally {
                                    close(rs);
                                    close(stat);
                                    stat = null;
                                    rs = null;
                                }
                            }
                        }
                    }
                } catch (SQLTimeoutException sqlt) {
                    String msgDesc = "Time Out, Unable to execute SQL [" + sql
                            + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqlt);
                    hdpException.generateResponseDataMap(false, getMessage(sqlt),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getColumns() Error : ", sqlt);
                    }
                    throw hdpException;
                } catch (SQLException sqle) {
                    String msgDesc = "Unable to execute SQL [" + sql + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqle);
                    hdpException.generateResponseDataMap(false, getMessage(sqle),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getColumns() Error : ", sqle);
                    }
                    throw hdpException;
                }
            }
        }
        return ret;
    }

    public List<String> getColumnList(String needle, List<String> catalogs, List<String> schemas, List<String> tables,
                                      List<String> columns) throws HadoopException {
        final String ndl = needle;
        final List<String> cats = catalogs;
        final List<String> shms = schemas;
        final List<String> tbls = tables;
        final List<String> cols = columns;

        List<String> columnList = Subject.doAs(getLoginSubject(), new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                List<String> ret = null;
                try {
                    ret = getColumns(ndl, cats, shms, tbls, cols);
                } catch (HadoopException he) {
                    LOG.error("<== StarRocksClient.getColumnList() :Unable to get the Column List", he);
                    throw he;
                }
                return ret;
            }
        });
        return columnList;
    }

    public List<String> getViewList(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            if (catalogs != null && !catalogs.isEmpty()
                    && schemas != null && !schemas.isEmpty()) {
                try {
                    for (String catalog : catalogs) {
                        for (String schema : schemas) {
                            try {
                                sql = "SELECT TABLE_NAME FROM information_schema.views WHERE TABLE_SCHEMA = '"
                                        + StringEscapeUtils.escapeSql(schema) + "'";

                                if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                                    sql += " AND TABLE_NAME LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                                }
                                stat = con.createStatement();
                                rs = stat.executeQuery(sql);
                                while (rs.next()) {
                                    String table = rs.getString(1);
                                    if (tables != null && tables.contains(table)) {
                                        continue;
                                    }
                                    ret.add(table);
                                }
                            } finally {
                                close(rs);
                                close(stat);
                                rs = null;
                                stat = null;
                            }
                        }
                    }
                } catch (SQLTimeoutException sqlt) {
                    String msgDesc = "Time Out, Unable to execute SQL [" + sql
                            + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqlt);
                    hdpException.generateResponseDataMap(false, getMessage(sqlt),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                    }
                    throw hdpException;
                } catch (SQLException sqle) {
                    String msgDesc = "Unable to execute SQL [" + sql + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqle);
                    hdpException.generateResponseDataMap(false, getMessage(sqle),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                    }
                    throw hdpException;
                }
            }
        }
        return ret;
    }

    public List<String> getMaterializedViewList(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            if (catalogs != null && !catalogs.isEmpty()
                    && schemas != null && !schemas.isEmpty()) {
                try {
                    for (String catalog : catalogs) {
                        for (String schema : schemas) {
                            try {
                                sql = "SELECT TABLE_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = '"
                                        + StringEscapeUtils.escapeSql(schema) + "'";

                                if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                                    sql += " AND TABLE_NAME LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                                }
                                stat = con.createStatement();
                                rs = stat.executeQuery(sql);
                                while (rs.next()) {
                                    String table = rs.getString(1);
                                    if (tables != null && tables.contains(table)) {
                                        continue;
                                    }
                                    ret.add(table);
                                }
                            } finally {
                                close(rs);
                                close(stat);
                                rs = null;
                                stat = null;
                            }
                        }
                    }
                } catch (SQLTimeoutException sqlt) {
                    String msgDesc = "Time Out, Unable to execute SQL [" + sql
                            + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqlt);
                    hdpException.generateResponseDataMap(false, getMessage(sqlt),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                    }
                    throw hdpException;
                } catch (SQLException sqle) {
                    String msgDesc = "Unable to execute SQL [" + sql + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqle);
                    hdpException.generateResponseDataMap(false, getMessage(sqle),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                    }
                    throw hdpException;
                }
            }
        }
        return ret;
    }

    public List<String> getFunctionList(String needle, List<String> catalogs, List<String> schemas, List<String> tables)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            if (catalogs != null && !catalogs.isEmpty()
                    && schemas != null && !schemas.isEmpty()) {
                try {
                    for (String catalog : catalogs) {
                        for (String schema : schemas) {
                            try {
                                sql = "SHOW FULL FUNCTIONS FROM `" + schema + "`";

                                if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                                    sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                                }
                                stat = con.createStatement();
                                rs = stat.executeQuery(sql);
                                while (rs.next()) {
                                    String table = rs.getString(1);
                                    if (tables != null && tables.contains(table)) {
                                        continue;
                                    }
                                    ret.add(table);
                                }
                            } finally {
                                close(rs);
                                close(stat);
                                rs = null;
                                stat = null;
                            }
                        }
                    }
                } catch (SQLTimeoutException sqlt) {
                    String msgDesc = "Time Out, Unable to execute SQL [" + sql
                            + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqlt);
                    hdpException.generateResponseDataMap(false, getMessage(sqlt),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                    }
                    throw hdpException;
                } catch (SQLException sqle) {
                    String msgDesc = "Unable to execute SQL [" + sql + "].";
                    HadoopException hdpException = new HadoopException(msgDesc,
                            sqle);
                    hdpException.generateResponseDataMap(false, getMessage(sqle),
                            msgDesc + ERR_MSG, null, null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                    }
                    throw hdpException;
                }
            }
        }
        return ret;
    }

    public List<String> getFunctionList(String needle, List<String> functions)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                try {
                    sql = "SHOW FULL GLOBAL FUNCTIONS";

                    if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                        sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                    }
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        String table = rs.getString(1);
                        if (functions != null && functions.contains(table)) {
                            continue;
                        }
                        ret.add(table);
                    }
                } finally {
                    close(rs);
                    close(stat);
                    rs = null;
                    stat = null;
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                }
                throw hdpException;
            }
        }
        return ret;
    }

    public List<String> getResourceList(String needle, List<String> functions)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                try {
                    sql = "SHOW RESOURCES";
                    /*
                    if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                        sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                    }

                     */
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        String table = rs.getString(1);
                        if (functions != null && functions.contains(table)) {
                            continue;
                        }
                        ret.add(table);
                    }
                } finally {
                    close(rs);
                    close(stat);
                    rs = null;
                    stat = null;
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                }
                throw hdpException;
            }
        }
        return ret;
    }

    public List<String> getResourceGroupList(String needle, List<String> functions)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                try {
                    sql = "SHOW RESOURCE GROUPS";
                    /*
                    if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                        sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                    }

                     */
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        String table = rs.getString(1);
                        if (functions != null && functions.contains(table)) {
                            continue;
                        }
                        ret.add(table);
                    }
                } finally {
                    close(rs);
                    close(stat);
                    rs = null;
                    stat = null;
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                }
                throw hdpException;
            }
        }
        return ret;
    }

    public List<String> getStorageVolumeList(String needle, List<String> functions)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                try {
                    sql = "SHOW STORAGE VOLUMES";

                    if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                        sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                    }
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        String table = rs.getString(1);
                        if (functions != null && functions.contains(table)) {
                            continue;
                        }
                        ret.add(table);
                    }
                } finally {
                    close(rs);
                    close(stat);
                    rs = null;
                    stat = null;
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                }
                throw hdpException;
            }
        }
        return ret;
    }

    public List<String> getUserList(String needle, List<String> users)
            throws HadoopException {
        List<String> ret = new ArrayList<>();
        if (con != null) {
            Statement stat = null;
            ResultSet rs = null;
            String sql = null;

            try {
                try {
                    sql = "show users";
                    /*
                    if (needle != null && !needle.isEmpty() && !needle.equals("*")) {
                        sql += " LIKE '" + StringEscapeUtils.escapeSql(needle) + "%'";
                    }
                     */
                    stat = con.createStatement();
                    rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        String originUser = rs.getString(1);
                        String user = originUser.split("@")[0].replace("'", "");
                        if (users != null && users.contains(user)) {
                            continue;
                        }
                        ret.add(user);
                    }
                } finally {
                    close(rs);
                    close(stat);
                    rs = null;
                    stat = null;
                }
            } catch (SQLTimeoutException sqlt) {
                String msgDesc = "Time Out, Unable to execute SQL [" + sql
                        + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqlt);
                hdpException.generateResponseDataMap(false, getMessage(sqlt),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqlt);
                }
                throw hdpException;
            } catch (SQLException sqle) {
                String msgDesc = "Unable to execute SQL [" + sql + "].";
                HadoopException hdpException = new HadoopException(msgDesc,
                        sqle);
                hdpException.generateResponseDataMap(false, getMessage(sqle),
                        msgDesc + ERR_MSG, null, null);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== StarRocksClient.getTables() Error : ", sqle);
                }
                throw hdpException;
            }
        }
        return ret;
    }

    public static Map<String, Object> connectionTest(String serviceName,
                                                     Map<String, String> connectionProperties)
            throws Exception {
        StarRocksClient client = null;
        Map<String, Object> resp = new HashMap<String, Object>();

        boolean status = false;

        List<String> testResult = null;

        try {
            client = new StarRocksClient(serviceName, connectionProperties);
            if (client != null) {
                testResult = client.getCatalogList("*", null);
                if (testResult != null && testResult.size() != 0) {
                    status = true;
                }
            }

            if (status) {
                String msg = "Connection test successful";
                generateResponseDataMap(status, msg, msg, null, null, resp);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (client != null) {
                client.close();
            }
        }

        return resp;
    }

    public void close() {
        Subject.doAs(getLoginSubject(), new PrivilegedAction<Void>() {
            public Void run() {
                close(con);
                return null;
            }
        });
    }

    private void close(Connection con) {
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            LOG.error("Unable to close StarRocks SQL connection", e);
        }
    }

    public void close(Statement stat) {
        try {
            if (stat != null) {
                stat.close();
            }
        } catch (SQLException e) {
            LOG.error("Unable to close SQL statement", e);
        }
    }

    public void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            LOG.error("Unable to close ResultSet", e);
        }
    }
}
