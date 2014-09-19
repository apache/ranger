DECLARE
	v_column_exists number := 0;
BEGIN
  Select count(*) into v_column_exists
    from user_tab_cols
    where column_name = upper('group_src')
      and table_name = upper('x_group');

  if (v_column_exists = 0) then
      execute immediate 'ALTER TABLE x_group ADD group_src NUMBER(10) DEFAULT 0 NOT NULL';
      commit;
  end if;
end;
/
