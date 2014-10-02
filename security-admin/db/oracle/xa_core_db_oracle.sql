/* create or replace procedure */
commit;
CREATE OR REPLACE PROCEDURE sp_dropobject(ObjName IN varchar2,ObjType IN varchar2)
IS
v_counter integer;
BEGIN
if (ObjType = 'TABLE') then
    select count(*) into v_counter from user_tables where table_name = upper(ObjName);
    if (v_counter > 0) then
      execute immediate 'drop table ' || ObjName || ' cascade constraints';
    end if;
end if;
  if (ObjType = 'PROCEDURE') then
    select count(*) into v_counter from User_Objects where object_type = 'PROCEDURE' and OBJECT_NAME = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP PROCEDURE ' || ObjName;
      end if;
  end if;
  if (ObjType = 'FUNCTION') then
    select count(*) into v_counter from User_Objects where object_type = 'FUNCTION' and OBJECT_NAME = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP FUNCTION ' || ObjName;
      end if;
  end if;
  if (ObjType = 'TRIGGER') then
    select count(*) into v_counter from User_Triggers where TRIGGER_NAME = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP TRIGGER ' || ObjName;
      end if;
  end if;
  if (ObjType = 'VIEW') then
    select count(*) into v_counter from User_Views where VIEW_NAME = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP VIEW ' || ObjName;
      end if;
  end if;
  if (ObjType = 'SEQUENCE') then
    select count(*) into v_counter from user_sequences where sequence_name = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP SEQUENCE ' || ObjName;
      end if;
  end if;
  if (ObjType = 'INDEX') then
    select count(*) into v_counter from user_indexes where index_name = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP INDEX ' || ObjName;
      end if;
  end if;
  if (ObjType = 'CONSTRAINT') then
    select count(*) into v_counter from user_constraints where constraint_name = upper(ObjName);
      if (v_counter > 0) then
        execute immediate 'DROP CONSTRAINT ' || ObjName;
      end if;
  end if;
END;
/
/* sequence */
call sp_dropobject('SEQ_GEN_IDENTITY','SEQUENCE');
call sp_dropobject('X_ACCESS_AUDIT_SEQ','SEQUENCE');
call sp_dropobject('X_ASSET_SEQ','SEQUENCE');
call sp_dropobject('X_AUDIT_MAP_SEQ','SEQUENCE');
call sp_dropobject('X_AUTH_SESS_SEQ','SEQUENCE');
call sp_dropobject('X_CRED_STORE_SEQ','SEQUENCE');
call sp_dropobject('X_DB_BASE_SEQ','SEQUENCE');
call sp_dropobject('X_GROUP_SEQ','SEQUENCE');
call sp_dropobject('X_GROUP_GROUPS_SEQ','SEQUENCE');
call sp_dropobject('X_GROUP_USERS_SEQ','SEQUENCE');
call sp_dropobject('X_PERM_MAP_SEQ','SEQUENCE');
call sp_dropobject('X_POLICY_EXPORT_SEQ','SEQUENCE');
call sp_dropobject('X_PORTAL_USER_SEQ','SEQUENCE');
call sp_dropobject('X_PORTAL_USER_ROLE_SEQ','SEQUENCE');
call sp_dropobject('X_RESOURCE_SEQ','SEQUENCE');
call sp_dropobject('X_TRX_LOG_SEQ','SEQUENCE');
call sp_dropobject('X_USER_SEQ','SEQUENCE');
call sp_dropobject('X_DB_VERSION_H_SEQ','SEQUENCE');
call sp_dropobject('V_TRX_LOG_SEQ','SEQUENCE');

commit;

/* drop table */
call sp_dropobject('vx_trx_log','VIEW');
call sp_dropobject('x_perm_map','TABLE');
call sp_dropobject('x_audit_map','TABLE');
call sp_dropobject('x_trx_log','TABLE');
call sp_dropobject('x_resource','TABLE');
call sp_dropobject('x_policy_export_audit','TABLE');
call sp_dropobject('x_group_users','TABLE');
call sp_dropobject('x_user','TABLE');
call sp_dropobject('x_group_groups','TABLE');
call sp_dropobject('X_GROUP','TABLE');
call sp_dropobject('x_db_base','TABLE');
call sp_dropobject('x_cred_store','TABLE');
call sp_dropobject('x_auth_sess','TABLE');
call sp_dropobject('x_asset','TABLE');
call sp_dropobject('xa_access_audit','TABLE');
call sp_dropobject('x_portal_user_role','TABLE');
call sp_dropobject('x_portal_user','TABLE');

commit;
/* create sequences */
CREATE SEQUENCE SEQ_GEN_IDENTITY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ACCESS_AUDIT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_ASSET_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_AUDIT_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_AUTH_SESS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_CRED_STORE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_DB_BASE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_GROUPS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_GROUP_USERS_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PERM_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_POLICY_EXPORT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PORTAL_USER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_PORTAL_USER_ROLE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_RESOURCE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_TRX_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_USER_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE X_DB_VERSION_H_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE V_TRX_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
commit;


/* create tables */
CREATE TABLE x_portal_user (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	first_name VARCHAR(256) DEFAULT NULL NULL ,
	last_name VARCHAR(256) DEFAULT NULL NULL ,
	pub_scr_name VARCHAR(2048) DEFAULT NULL NULL ,
	login_id VARCHAR(767) DEFAULT NULL NULL ,
	password VARCHAR(512) NOT NULL,
	email VARCHAR(512) DEFAULT NULL NULL ,
	status NUMBER(11) DEFAULT '0' NOT NULL ,
	user_src NUMBER(11) DEFAULT '0' NOT NULL ,
	notes VARCHAR(4000) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_portal_user_UK_login_id UNIQUE (login_id) ,
	CONSTRAINT x_portal_user_UK_email UNIQUE (email),
	CONSTRAINT x_portal_user_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_portal_user_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_portal_user_role (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE  DEFAULT NULL NULL ,
	added_by_id NUMBER(20)  DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	user_id NUMBER(20) NOT NULL ,
	user_role VARCHAR(128)  DEFAULT NULL NULL ,
	status NUMBER(11) DEFAULT 0 NOT NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_portal_user_role_FK_addedby FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_portal_user_role_FK_updby FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_portal_user_role_FK_user_id FOREIGN KEY (user_id) REFERENCES x_portal_user (id)
);

CREATE TABLE xa_access_audit (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	audit_type NUMBER(11) DEFAULT '0' NOT NULL ,
	access_result NUMBER(11) DEFAULT '0' NULL ,
	access_type VARCHAR(255) DEFAULT NULL NULL ,
	acl_enforcer VARCHAR(255) DEFAULT NULL NULL ,
	agent_id VARCHAR(255) DEFAULT NULL NULL ,
	client_ip VARCHAR(255) DEFAULT NULL NULL ,
	client_type VARCHAR(255) DEFAULT NULL NULL ,
	policy_id NUMBER(20) DEFAULT '0' NULL ,
	repo_name VARCHAR(255) DEFAULT NULL NULL ,
	repo_type NUMBER(11) DEFAULT '0' NULL,
	result_reason VARCHAR(255) DEFAULT NULL NULL ,
	session_id VARCHAR(255) DEFAULT NULL NULL ,
	event_time DATE DEFAULT NULL NULL ,
	request_user VARCHAR(255) DEFAULT NULL NULL ,
	action VARCHAR(2000) DEFAULT NULL NULL ,
	request_data VARCHAR(2000) DEFAULT NULL NULL ,
	resource_path VARCHAR(2000) DEFAULT NULL NULL ,
	resource_type VARCHAR(255) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT xa_access_audit_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT xa_access_audit_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_asset (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	asset_name VARCHAR(1024) NOT NULL,
	descr VARCHAR(4000) DEFAULT NULL NULL,
	act_status NUMBER(11) DEFAULT '0' NOT NULL ,
	asset_type NUMBER(11)  DEFAULT '0' NOT NULL,
	config CLOB NULL,
	sup_native NUMBER(1) DEFAULT '0' NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT x_asset_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_asset_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_auth_sess (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	login_id VARCHAR(767) NOT NULL,
	user_id NUMBER(20) DEFAULT NULL NULL ,
	ext_sess_id VARCHAR(512) DEFAULT NULL NULL ,
	auth_time DATE NOT NULL,
	auth_status NUMBER(11) DEFAULT '0' NOT NULL ,
	auth_type NUMBER(11) DEFAULT '0' NOT NULL ,
	auth_provider NUMBER(11) DEFAULT '0' NOT NULL ,
	device_type NUMBER(11) DEFAULT '0' NOT NULL ,
	req_ip VARCHAR(48) NOT NULL,
	req_ua VARCHAR(1024) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_auth_sess_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_auth_sess_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_auth_sess_FK_user_id FOREIGN KEY (user_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_cred_store (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	store_name VARCHAR(1024) NOT NULL,
	descr VARCHAR(4000) NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT x_cred_store_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_cred_store_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_db_base (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_db_base_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_db_base_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE X_GROUP(
	ID NUMBER(20,0) NOT NULL ENABLE,
	CREATE_TIME DATE DEFAULT NULL,
	UPDATE_TIME DATE DEFAULT NULL,
	ADDED_BY_ID NUMBER(20,0) DEFAULT NULL,
	UPD_BY_ID NUMBER(20,0) DEFAULT NULL,
	GROUP_NAME VARCHAR2(1024) NOT NULL ENABLE,
	DESCR VARCHAR2(4000) DEFAULT NULL NULL,
	STATUS NUMBER(11,0) DEFAULT '0' NOT NULL ENABLE,
	GROUP_TYPE NUMBER(11,0) DEFAULT '0' NOT NULL ENABLE,
	CRED_STORE_ID NUMBER(20,0) DEFAULT NULL,
	PRIMARY KEY (ID),
	CONSTRAINT X_GROUP_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
	CONSTRAINT X_GROUP_FK_CRED_STORE_ID FOREIGN KEY (CRED_STORE_ID) REFERENCES X_CRED_STORE (ID) ENABLE,
	CONSTRAINT X_GROUP_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE
) ;

CREATE TABLE x_group_groups (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	group_name VARCHAR(1024) NOT NULL,
	p_group_id NUMBER(20) DEFAULT NULL NULL ,
	group_id NUMBER(20) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_group_groups_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_group_groups_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group (id),
	CONSTRAINT x_group_groups_FK_p_group_id FOREIGN KEY (p_group_id) REFERENCES x_group (id),
	CONSTRAINT x_group_groups_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_user (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	user_name VARCHAR(1024) NOT NULL,
	descr VARCHAR(4000) DEFAULT NULL  NULL,
	status NUMBER(11) DEFAULT '0' NOT NULL,
	cred_store_id NUMBER(20) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_user_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_user_FK_cred_store_id FOREIGN KEY (cred_store_id) REFERENCES x_cred_store (id),
	CONSTRAINT x_user_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_group_users (
	id NUMBER(20) NOT NULL ,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	group_name VARCHAR(1024) NOT NULL,
	p_group_id NUMBER(20) DEFAULT NULL NULL ,
	user_id NUMBER(20) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_group_users_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_group_users_FK_p_group_id FOREIGN KEY (p_group_id) REFERENCES x_group (id),
	CONSTRAINT x_group_users_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_group_users_FK_user_id FOREIGN KEY (user_id) REFERENCES x_user (id)
);

CREATE TABLE x_policy_export_audit (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	client_ip VARCHAR(255) NOT NULL,
	agent_id VARCHAR(255) DEFAULT NULL NULL ,
	req_epoch NUMBER(20) NOT NULL,
	last_updated DATE DEFAULT NULL NULL ,
	repository_name VARCHAR(1024) DEFAULT NULL NULL ,
	exported_json CLOB NULL,
	http_ret_code NUMBER(11) DEFAULT '0' NOT NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_policy_export_audit_FK_added FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_policy_export_audit_FK_upd FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);

CREATE TABLE x_resource (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	res_name VARCHAR(4000) DEFAULT NULL NULL ,
	descr VARCHAR(4000) DEFAULT NULL NULL ,
	res_type NUMBER(11) DEFAULT '0' NOT NULL ,
	asset_id NUMBER(20) NOT NULL,
	parent_id NUMBER(20) DEFAULT NULL NULL ,
	parent_path VARCHAR(4000) DEFAULT NULL NULL ,
	is_encrypt NUMBER(11) DEFAULT '0' NOT NULL ,
	is_recursive NUMBER(11) DEFAULT '0' NOT NULL ,
	res_group VARCHAR(1024) DEFAULT NULL NULL ,
	res_dbs CLOB NULL,
	res_tables CLOB NULL,
	res_col_fams CLOB NULL,
	res_cols CLOB NULL,
	res_udfs CLOB NULL,
	res_status NUMBER(11) DEFAULT '1' NOT NULL,
	table_type NUMBER(11) DEFAULT '0' NOT NULL,
	col_type NUMBER(11) DEFAULT '0' NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT x_resource_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_resource_FK_asset_id FOREIGN KEY (asset_id) REFERENCES x_asset (id),
	CONSTRAINT x_resource_FK_parent_id FOREIGN KEY (parent_id) REFERENCES x_resource (id),
	CONSTRAINT x_resource_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);


CREATE TABLE x_trx_log (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	class_type NUMBER(11) DEFAULT '0' NOT NULL ,
	object_id NUMBER(20) DEFAULT NULL NULL ,
	parent_object_id NUMBER(20) DEFAULT NULL NULL ,
	parent_object_class_type NUMBER(11) DEFAULT '0' NOT NULL ,
	parent_object_name VARCHAR(1024) DEFAULT NULL NULL ,
	object_name VARCHAR(1024) DEFAULT NULL NULL ,
	attr_name VARCHAR(255) DEFAULT NULL NULL ,
	prev_val CLOB DEFAULT NULL NULL ,
	new_val CLOB DEFAULT NULL NULL ,
	trx_id VARCHAR(1024) DEFAULT NULL NULL ,
	action VARCHAR(255) DEFAULT NULL NULL ,
	sess_id VARCHAR(512) DEFAULT NULL NULL ,
	req_id VARCHAR(30) DEFAULT NULL NULL ,
	sess_type VARCHAR(30) DEFAULT NULL NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_trx_log_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_trx_log_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id)
);
CREATE TABLE x_perm_map (
	id NUMBER(20) NOT NULL,
	create_time DATE DEFAULT NULL NULL ,
	update_time DATE DEFAULT NULL NULL ,
	added_by_id NUMBER(20) DEFAULT NULL NULL ,
	upd_by_id NUMBER(20) DEFAULT NULL NULL ,
	perm_group VARCHAR(1024) DEFAULT NULL NULL ,
	res_id NUMBER(20) DEFAULT NULL NULL ,
	group_id NUMBER(20) DEFAULT NULL NULL ,
	user_id NUMBER(20) DEFAULT NULL NULL ,
	perm_for NUMBER(11) DEFAULT '0' NOT NULL ,
	perm_type NUMBER(11) DEFAULT '0' NOT NULL ,
	is_recursive NUMBER(11) DEFAULT '0' NOT NULL ,
	is_wild_card NUMBER(1) DEFAULT '1' NOT NULL ,
	grant_revoke NUMBER(1) DEFAULT '1' NOT NULL ,
	PRIMARY KEY (id),
	CONSTRAINT x_perm_map_FK_added_by_id FOREIGN KEY (added_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_perm_map_FK_group_id FOREIGN KEY (group_id) REFERENCES x_group (id),
	CONSTRAINT x_perm_map_FK_res_id FOREIGN KEY (res_id) REFERENCES x_resource (id),
	CONSTRAINT x_perm_map_FK_upd_by_id FOREIGN KEY (upd_by_id) REFERENCES x_portal_user (id),
	CONSTRAINT x_perm_map_FK_user_id FOREIGN KEY (user_id) REFERENCES x_user (id)
);

CREATE TABLE X_AUDIT_MAP
(	ID NUMBER(20,0) NOT NULL ENABLE,
	CREATE_TIME DATE DEFAULT NULL,
	UPDATE_TIME DATE DEFAULT NULL,
	ADDED_BY_ID NUMBER(20,0) DEFAULT NULL,
	UPD_BY_ID NUMBER(20,0) DEFAULT NULL,
	RES_ID NUMBER(20,0) DEFAULT NULL,
	GROUP_ID NUMBER(20,0) DEFAULT NULL,
	USER_ID NUMBER(20,0) DEFAULT NULL,
	AUDIT_TYPE NUMBER(11,0) DEFAULT 0 NOT NULL ENABLE,
	PRIMARY KEY (ID),
	CONSTRAINT X_AUDIT_MAP_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
	CONSTRAINT X_AUDIT_MAP_FK_GROUP_ID FOREIGN KEY (GROUP_ID) REFERENCES X_GROUP (ID) ENABLE,
	CONSTRAINT X_AUDIT_MAP_FK_RES_ID FOREIGN KEY (RES_ID) REFERENCES X_RESOURCE (ID) ENABLE,
	CONSTRAINT X_AUDIT_MAP_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER (ID) ENABLE,
	CONSTRAINT X_AUDIT_MAP_FK_USER_ID FOREIGN KEY (USER_ID) REFERENCES X_USER (ID) ENABLE
);
commit;
CREATE VIEW vx_trx_log AS select x_trx_log.id AS id,x_trx_log.create_time AS create_time,x_trx_log.update_time AS update_time,x_trx_log.added_by_id AS added_by_id,x_trx_log.upd_by_id AS upd_by_id,x_trx_log.class_type AS class_type,x_trx_log.object_id AS object_id,x_trx_log.parent_object_id AS parent_object_id,x_trx_log.parent_object_class_type AS parent_object_class_type,x_trx_log.attr_name AS attr_name,x_trx_log.parent_object_name AS parent_object_name,x_trx_log.object_name AS object_name,x_trx_log.prev_val AS prev_val,x_trx_log.new_val AS new_val,x_trx_log.trx_id AS trx_id,x_trx_log.action AS action,x_trx_log.sess_id AS sess_id,x_trx_log.req_id AS req_id,x_trx_log.sess_type AS sess_type from x_trx_log  where id in(select min(x_trx_log.id) from x_trx_log group by x_trx_log.trx_id);
commit;

CREATE  INDEX xa_access_audit_FK_added_by_id ON  xa_access_audit(added_by_id);
CREATE  INDEX xa_access_audit_FK_upd_by_id ON  xa_access_audit(upd_by_id);
CREATE  INDEX xa_access_audit_cr_time ON  xa_access_audit(create_time);
CREATE  INDEX xa_access_audit_up_time ON  xa_access_audit(update_time);
CREATE  INDEX xa_access_audit_event_time ON  xa_access_audit(event_time);
CREATE  INDEX x_asset_FK_added_by_id ON  x_asset(added_by_id);
CREATE  INDEX x_asset_FK_upd_by_id ON  x_asset(upd_by_id);
CREATE  INDEX x_asset_cr_time ON x_asset (create_time);
CREATE  INDEX x_asset_up_time ON x_asset (update_time);
CREATE  INDEX x_audit_map_FK_added_by_id ON x_audit_map (added_by_id);
CREATE  INDEX x_audit_map_FK_upd_by_id ON x_audit_map (upd_by_id);
CREATE  INDEX x_audit_map_FK_res_id ON  x_audit_map(res_id);
CREATE  INDEX x_audit_map_FK_group_id ON x_audit_map (group_id);
CREATE  INDEX x_audit_map_FK_user_id ON  x_audit_map(user_id);
CREATE  INDEX x_audit_map_cr_time ON  x_audit_map(create_time);
CREATE  INDEX x_audit_map_up_time ON x_audit_map (update_time);
CREATE  INDEX x_auth_sess_FK_added_by_id ON x_auth_sess (added_by_id);
CREATE  INDEX x_auth_sess_FK_upd_by_id ON x_auth_sess (upd_by_id);
CREATE  INDEX x_auth_sess_FK_user_id ON x_auth_sess (user_id);
CREATE  INDEX x_auth_sess_cr_time ON x_auth_sess (create_time);
CREATE  INDEX x_auth_sess_up_time ON x_auth_sess (update_time);
CREATE  INDEX x_cred_store_FK_added_by_id ON x_cred_store (added_by_id);
CREATE  INDEX x_cred_store_FK_upd_by_id ON x_cred_store (upd_by_id);
CREATE  INDEX x_cred_store_cr_time ON x_cred_store (create_time);
CREATE  INDEX x_cred_store_up_time ON x_cred_store (update_time);
CREATE  INDEX x_db_base_FK_added_by_id ON x_db_base (added_by_id);
CREATE  INDEX x_db_base_FK_upd_by_id ON x_db_base (upd_by_id);
CREATE  INDEX x_db_base_cr_time ON x_db_base (create_time);
CREATE  INDEX x_db_base_up_time ON  x_db_base(update_time);
CREATE  INDEX x_group_FK_added_by_id ON x_group (added_by_id);
CREATE  INDEX x_group_FK_upd_by_id ON x_group (upd_by_id);
CREATE  INDEX x_group_FK_cred_store_id ON x_group (cred_store_id);
CREATE  INDEX x_group_cr_time ON x_group (create_time);
CREATE  INDEX x_group_up_time ON x_group (update_time);
CREATE  INDEX x_group_groups_FK_added_by_id ON x_group_groups (added_by_id);
CREATE  INDEX x_group_groups_FK_upd_by_id ON  x_group_groups(upd_by_id);
CREATE  INDEX x_group_groups_FK_p_group_id ON x_group_groups (p_group_id);
CREATE  INDEX x_group_groups_FK_group_id ON  x_group_groups(group_id);
CREATE  INDEX x_group_groups_cr_time ON x_group_groups (create_time);
CREATE  INDEX x_group_groups_up_time ON x_group_groups (update_time);
CREATE  INDEX x_group_users_FK_added_by_id ON x_group_users (added_by_id);
CREATE  INDEX x_group_users_FK_upd_by_id ON  x_group_users(upd_by_id);
CREATE  INDEX x_group_users_FK_p_group_id ON x_group_users (p_group_id);
CREATE  INDEX x_group_users_FK_user_id ON x_group_users (user_id);
CREATE  INDEX x_group_users_cr_time ON  x_group_users(create_time);
CREATE  INDEX x_group_users_up_time ON  x_group_users(update_time);
CREATE  INDEX x_perm_map_FK_added_by_id ON x_perm_map (added_by_id);
CREATE  INDEX x_perm_map_FK_upd_by_id ON x_perm_map (upd_by_id);
CREATE  INDEX x_perm_map_FK_res_id ON  x_perm_map(res_id);
CREATE  INDEX x_perm_map_FK_group_id ON  x_perm_map(group_id);
CREATE  INDEX x_perm_map_FK_user_id ON  x_perm_map(user_id);
CREATE  INDEX x_perm_map_cr_time ON x_perm_map (create_time);
CREATE  INDEX x_perm_map_up_time ON  x_perm_map(update_time);
CREATE  INDEX x_policy_export_audit_FK_added ON x_policy_export_audit (added_by_id);
CREATE  INDEX x_policy_export_audit_FK_upd ON x_policy_export_audit (upd_by_id);
CREATE  INDEX x_policy_export_audit_cr_time ON x_policy_export_audit (create_time);
CREATE  INDEX x_policy_export_audit_up_time ON  x_policy_export_audit(update_time);
CREATE  INDEX x_portal_user_FK_added_by_id ON x_portal_user (added_by_id);
CREATE  INDEX x_portal_user_FK_upd_by_id ON x_portal_user (upd_by_id);
CREATE  INDEX x_portal_user_cr_time ON  x_portal_user(create_time);
CREATE  INDEX x_portal_user_up_time ON x_portal_user (update_time);
CREATE  INDEX x_portal_user_name ON  x_portal_user(first_name);
CREATE  INDEX x_portal_user_role_FK_added ON  x_portal_user_role(added_by_id);
CREATE  INDEX x_portal_user_role_FK_upd ON  x_portal_user_role(upd_by_id);
CREATE  INDEX x_portal_user_role_FK_user_id ON  x_portal_user_role(user_id);
CREATE  INDEX x_portal_user_role_cr_time ON  x_portal_user_role(create_time);
CREATE  INDEX x_portal_user_role_up_time ON x_portal_user_role (update_time);
CREATE  INDEX x_resource_FK_added_by_id ON  x_resource(added_by_id);
CREATE  INDEX x_resource_FK_upd_by_id ON x_resource(upd_by_id);
CREATE  INDEX x_resource_FK_asset_id ON x_resource (asset_id);
CREATE  INDEX x_resource_FK_parent_id ON x_resource (parent_id);
CREATE  INDEX x_resource_cr_time ON  x_resource(create_time);
CREATE  INDEX x_resource_up_time ON x_resource (update_time);
CREATE  INDEX x_trx_log_FK_added_by_id ON x_trx_log (added_by_id);
CREATE  INDEX x_trx_log_FK_upd_by_id ON  x_trx_log(upd_by_id);
CREATE  INDEX x_trx_log_cr_time ON x_trx_log (create_time);
CREATE  INDEX x_trx_log_up_time ON x_trx_log (update_time);
CREATE  INDEX x_user_FK_added_by_id ON x_user (added_by_id);
CREATE  INDEX x_user_FK_upd_by_id ON x_user (upd_by_id);
CREATE  INDEX x_user_FK_cred_store_id ON x_user (cred_store_id);
CREATE  INDEX x_user_cr_time ON x_user (create_time);
CREATE  INDEX x_user_up_time ON  x_user(update_time);

insert into x_portal_user (
       id,CREATE_TIME, UPDATE_TIME,
       FIRST_NAME, LAST_NAME, PUB_SCR_NAME,
       LOGIN_ID, PASSWORD, EMAIL, STATUS
) values (
X_PORTAL_USER_SEQ.NEXTVAL, SYSDATE, SYSDATE,
 'Admin', '', 'Admin',
 'admin', 'ceb4f32325eda6142bd65215f4c0f371', '', 1
);

insert into x_portal_user_role (
      id, CREATE_TIME, UPDATE_TIME,
       USER_ID, USER_ROLE, STATUS
) values (
X_PORTAL_USER_ROLE_SEQ.NEXTVAL, SYSDATE, SYSDATE,
 1, 'ROLE_SYS_ADMIN', 1
);

insert into x_user (id,CREATE_TIME, UPDATE_TIME,user_name, status,descr) values (
X_USER_SEQ.NEXTVAL, SYSDATE, SYSDATE,'admin', 0,'Administrator');

INSERT INTO x_group (ID,ADDED_BY_ID, CREATE_TIME, DESCR, GROUP_TYPE, GROUP_NAME, STATUS, UPDATE_TIME, UPD_BY_ID) VALUES (X_GROUP_SEQ.nextval,1, sys_extract_utc(systimestamp), 'public group', 0, 'public', 0, sys_extract_utc(systimestamp), 1);
commit;



CREATE OR REPLACE TRIGGER x_auth_sess_trigger
  BEFORE INSERT
  ON x_auth_sess
  FOR EACH ROW
  -- Optionally restrict this trigger to fire only when really needed
  WHEN (new.id is null)
DECLARE
  v_id x_auth_sess.id%TYPE;
BEGIN
  -- Select a new value from the sequence into a local variable. As
  -- commented, this step is optional. You can directly select into :new.qname_id
  SELECT  x_auth_sess_seq.nextval INTO v_id FROM DUAL;

  -- :new references the record that you are about to insert into qname. Hence,
  -- you can overwrite the value of :new.qname_id (qname.qname_id) with the value
  -- obtained from your sequence, before inserting
  :new.id := v_id;
END my_trigger;
/
commit;
