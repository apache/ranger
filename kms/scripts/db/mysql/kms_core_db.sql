DROP TABLE IF EXISTS `ranger_masterkey`;
CREATE TABLE `ranger_masterkey` (
`id` bigint( 20 ) NOT NULL AUTO_INCREMENT ,
`create_time` datetime DEFAULT NULL ,
`update_time` datetime DEFAULT NULL ,
`added_by_id` bigint( 20 ) DEFAULT NULL ,
`upd_by_id` bigint( 20 ) DEFAULT NULL ,
`cipher` varchar( 255 ) DEFAULT NULL ,
`bitlength` int DEFAULT NULL ,
`masterkey` varchar(2048),
PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `ranger_keystore`;
CREATE TABLE `ranger_keystore` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `added_by_id` bigint(20) DEFAULT NULL,
  `upd_by_id` bigint(20) DEFAULT NULL,
  `kms_alias` varchar(255) NOT NULL,
  `kms_createdDate` bigint(20) DEFAULT NULL,
  `kms_cipher` varchar(255) DEFAULT NULL,
   `kms_bitLength` bigint(20) DEFAULT NULL,
  `kms_description` varchar(512) DEFAULT NULL,
  `kms_version` bigint(20) DEFAULT NULL,
  `kms_attributes` varchar(1024) DEFAULT NULL,
  `kms_encoded`varchar(2048),
  PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=latin1;
