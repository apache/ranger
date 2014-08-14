drop procedure if exists add_policy_name_column_to_x_resource_table;

delimiter ;;
create procedure add_policy_name_column_to_x_resource_table() begin

 /* add policy name column if not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource') then
  	if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource' and column_name = 'policy_name') then
  		ALTER TABLE  `x_resource` ADD  `policy_name` VARCHAR( 500 ) NULL DEFAULT NULL;
  		if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource' and column_name = 'policy_name') then
  			ALTER TABLE  `x_resource` ADD UNIQUE  `x_resource_UK_policy_name` (  `policy_name` );
  		end if;
 	end if;
 end if;

  
end;;

delimiter ;
call add_policy_name_column_to_x_resource_table();

drop procedure if exists add_policy_name_column_to_x_resource_table;