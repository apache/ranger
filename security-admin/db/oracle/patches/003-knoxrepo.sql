DECLARE
	v_column_exists number := 0;
BEGIN
  Select count(*) into v_column_exists
    from user_tab_cols
    where column_name = upper('res_topologies')
      and table_name = upper('x_resource');

  if (v_column_exists = 0) then
      execute immediate 'ALTER TABLE  x_resource ADD  res_topologies CLOB DEFAULT NULL NULL';
      commit;
  end if;
  v_column_exists:=0;
  Select count(*) into v_column_exists
    from user_tab_cols
    where column_name = upper('res_services')
      and table_name = upper('x_resource');

  if (v_column_exists = 0) then
      execute immediate 'ALTER TABLE  x_resource ADD  res_services CLOB DEFAULT NULL NULL';
      commit;
  end if;
  v_column_exists:=0;
Select count(*) into v_column_exists
    from user_tab_cols
    where column_name = upper('ip_address')
      and table_name = upper('x_perm_map');

  if (v_column_exists = 0) then
      execute immediate 'ALTER TABLE  x_perm_map ADD  ip_address CLOB DEFAULT NULL NULL';
      commit;
  end if;
end;
/
