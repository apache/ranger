drop procedure if exists change_values_columns_datatype_of_x_trx_log_table;

delimiter ;;
create procedure change_values_columns_datatype_of_x_trx_log_table() begin

 /* change prev_value column data type to mediumtext */
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_trx_log' and column_name = 'prev_val' and data_type='varchar') then
 	ALTER TABLE  `x_trx_log` CHANGE  `prev_val`  `prev_val` MEDIUMTEXT NULL DEFAULT NULL ;
 end if;
 if exists (select * from information_schema.columns where table_schema=database() and table_name = 'x_trx_log' and column_name = 'new_val'  and data_type='varchar') then
 	ALTER TABLE  `x_trx_log` CHANGE  `new_val`  `new_val` MEDIUMTEXT NULL DEFAULT NULL ;
 end if;
  
end;;

delimiter ;
call change_values_columns_datatype_of_x_trx_log_table();

drop procedure if exists change_values_columns_datatype_of_x_trx_log_table;
