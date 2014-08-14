drop procedure if exists insert_public_group_in_x_group_table;

delimiter ;;
create procedure insert_public_group_in_x_group_table() begin

 /* check table x_group exist or not */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_group') then
 	/* check record for group name public exist or not */
 	if not exists (select * from x_group where group_name = 'public') then
 		INSERT INTO x_group (ADDED_BY_ID, CREATE_TIME, DESCR, GROUP_SRC, GROUP_TYPE, GROUP_NAME, STATUS, UPDATE_TIME, UPD_BY_ID) VALUES (1, UTC_TIMESTAMP(), 'public group', 0, 0, 'public', 0, UTC_TIMESTAMP(), 1);
 	end if;
 end if;
  
end;;

delimiter ;
call insert_public_group_in_x_group_table();

drop procedure if exists insert_public_group_in_x_group_table;
