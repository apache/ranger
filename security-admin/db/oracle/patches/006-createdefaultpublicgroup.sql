DECLARE
	v_column_exists number := 0;
BEGIN
  Select count(*) into v_column_exists from user_tab_cols
	where column_name = upper('GROUP_NAME') and table_name = upper('x_group');
  if (v_column_exists = 0) then
	Select count(*) into v_column_exists from x_group where GROUP_NAME ='public';
	if (v_column_exists = 0) then
		execute immediate 'INSERT INTO x_group (ID,ADDED_BY_ID, CREATE_TIME, DESCR, GROUP_SRC, GROUP_TYPE, GROUP_NAME, STATUS, UPDATE_TIME, UPD_BY_ID) VALUES (X_GROUP_SEQ.nextval,1, sys_extract_utc(systimestamp), "public group", 0, 0, "public", 0, sys_extract_utc(systimestamp),1)';
		commit;
	end if;
  end if;
end;
/
