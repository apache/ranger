create table if not exists x_db_version_h  (
	id				bigint not null auto_increment primary key,
	version   		varchar(64) not null,
	inst_at 	    timestamp not null default current_timestamp,
	inst_by 	    varchar(256) not null,
	updated_at      timestamp not null,
    updated_by      varchar(256) not null,
	active          ENUM('Y', 'N') default 'Y'
) ;
