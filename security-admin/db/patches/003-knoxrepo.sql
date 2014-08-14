drop procedure if exists add_columns_for_knox_repository;

delimiter ;;
create procedure add_columns_for_knox_repository() begin

 /* add res_topologies if  not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource') then
 	if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource' and column_name = 'res_topologies') then
 		ALTER TABLE  `x_resource` ADD  `res_topologies` TEXT NULL DEFAULT NULL ;
 	end if;
 end if;
 
  /* add res_services if  not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource') then
 	if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_resource' and column_name = 'res_services') then
 		ALTER TABLE  `x_resource` ADD  `res_services` TEXT NULL DEFAULT NULL;
 	end if;
 end if;
 
  /* add ip_address if  not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_perm_map') then
 	if not exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_perm_map' and column_name = 'ip_address') then
 		ALTER TABLE  `x_perm_map` ADD  `ip_address` TEXT NULL DEFAULT NULL;
 	end if;
 end if;

  
end;;

delimiter ;
call add_columns_for_knox_repository();

drop procedure if exists add_columns_for_knox_repository;
