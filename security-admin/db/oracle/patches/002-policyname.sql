DECLARE
	v_column_exists number := 0;
BEGIN
  Select count(*) into v_column_exists
    from user_tab_cols
    where column_name = upper('policy_name')
      and table_name = upper('x_resource');
  if (v_column_exists = 0) then
      execute immediate 'ALTER TABLE  x_resource ADD  policy_name VARCHAR(500)  DEFAULT NULL NULL';
      execute immediate 'ALTER TABLE  x_resource ADD CONSTRAINT x_resource_UK_policy_name UNIQUE(policy_name)';
      commit;
  end if;
end;
/
