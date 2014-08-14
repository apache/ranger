 drop procedure if exists add_group_source_column_to_x_group_table;

delimiter ;;
 create procedure add_group_source_column_to_x_group_table() begin

 /* add group source column if not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_group') then
	if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_group' and column_name = 'group_src') then
		ALTER TABLE  `x_group` ADD  `group_src` INT NOT NULL DEFAULT 0;
 	end if;
 end if;
  
end;;

delimiter ;

 call add_group_source_column_to_x_group_table();

 drop procedure if exists add_group_source_column_to_x_group_table;