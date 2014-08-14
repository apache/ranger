drop procedure if exists change_config_column_datatype_of_x_asset_table;

delimiter ;;
create procedure change_config_column_datatype_of_x_asset_table() begin

 /* change config data type to longtext if not exist */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_asset' and column_name = 'config' and data_type='text') then
 	ALTER TABLE  `x_asset` CHANGE  `config`  `config` MEDIUMTEXT NULL DEFAULT NULL ;
 end if;
  
end;;

delimiter ;
call change_config_column_datatype_of_x_asset_table();

drop procedure if exists change_config_column_datatype_of_x_asset_table;
