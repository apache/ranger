create user 'xaadmin'@'%' identified by 'xaadmin';
GRANT ALL ON *.* TO 'xaadmin'@'localhost' IDENTIFIED BY 'xaadmin';
grant all privileges on *.* to 'xaadmin'@'%' with grant option;
grant all privileges on *.* to 'xaadmin'@'localhost' with grant option;
FLUSH PRIVILEGES;

