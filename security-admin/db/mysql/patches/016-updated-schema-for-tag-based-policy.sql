-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Temporary table structure for view `vx_trx_log`
--

-- -----------------------------------------------------
-- Table `x_tag_def`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_tag_def` ;

CREATE TABLE IF NOT EXISTS `x_tag_def` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `version` BIGINT(20) NULL,
  `name` VARCHAR(512) NOT NULL,
  `source` VARCHAR(128) NULL,
  `is_enabled` TINYINT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `guid_UNIQUE` (`guid` ASC),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC),
  INDEX `fk_X_TAG_DEF_NAME` (`name` ASC),
  INDEX `fk_X_TAG_DEF_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_TAG_DEF_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_TAG_DEF_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_DEF_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `x_tag`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_tag` ;

CREATE TABLE IF NOT EXISTS `x_tag` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `type` BIGINT(20) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_X_TAG_TYPE` (`type` ASC),
  INDEX `fk_X_TAG_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_TAG_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_TAG_TYPE`
    FOREIGN KEY (`type`)
    REFERENCES `x_tag_def` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_X_TAG_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;



-- -----------------------------------------------------
-- Table `x_service_resource`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_service_resource` ;

CREATE TABLE IF NOT EXISTS `x_service_resource` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `version` BIGINT(20) NULL,
  `service_id` BIGINT(20) NOT NULL,
  `resource_signature` varchar(128) DEFAULT NULL,
  `is_enabled` TINYINT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  INDEX `fk_X_SERVICE_RESOURCE_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_SERVICE_RESOURCE_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_SERVICE_RESOURCE_SERVICE_ID`
    FOREIGN KEY (`service_id`)
    REFERENCES `x_service` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RESOURCE_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RESOURCE_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `x_service_resource_element`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_service_resource_element` ;

CREATE TABLE IF NOT EXISTS `x_service_resource_element` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `res_id` BIGINT(20) NOT NULL,
  `res_def_id` BIGINT(20) NOT NULL,
  `is_excludes` TINYINT(1) NULL DEFAULT false,
  `is_recursive` TINYINT(1) NULL DEFAULT false,
  PRIMARY KEY (`id`),
  INDEX `fk_X_SERVICE_RESOURCE_ELEMENT_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_SERVICE_RESOURCE_ELEMENT_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_SERVICE_RESOURCE_ELEMENT_res_def_id` 
    FOREIGN KEY (`res_def_id`) 
    REFERENCES `x_resource_def` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RESOURCE_ELEMENT_res_id` 
    FOREIGN KEY (`res_id`) 
    REFERENCES `x_service_resource` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RESOURCE_ELEMENT_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RESOURCE_ELEMENT_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `x_tag_attr_def`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_tag_attr_def` ;

CREATE TABLE IF NOT EXISTS `x_tag_attr_def` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `tag_def_id` BIGINT(20) NOT NULL,
  `name` VARCHAR(512) NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_X_TAG_ATTR_DEF_TAG_DEF_ID` (`tag_def_id` ASC),
  INDEX `fk_X_TAG_ATTR_DEF_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_TAG_ATTR_DEF_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_TAG_ATTR_DEF_TAG_DEF_ID`
    FOREIGN KEY (`tag_def_id`)
    REFERENCES `x_tag_def` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_ATTR_DEF_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_ATTR_DEF_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `x_tag_attr`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_tag_attr` ;

CREATE TABLE IF NOT EXISTS `x_tag_attr` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `tag_id` BIGINT(20) NOT NULL,
  `attr_name` VARCHAR(128) NOT NULL,
  `attr_value` VARCHAR(512) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_X_TAG_ID` (`tag_id` ASC),
  INDEX `fk_X_TAG_ATTR_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_TAG_ATTR_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_TAG_ATTR_TAG_ID`
    FOREIGN KEY (`tag_id`)
    REFERENCES `x_tag` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_ATTR_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_ATTR_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `x_tag_resource_map`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_tag_resource_map` ;

CREATE TABLE IF NOT EXISTS `x_tag_resource_map` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `tag_id` BIGINT(20) NOT NULL,
  `res_id` BIGINT(20) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_X_TAG_ID` (`tag_id` ASC),
  INDEX `fk_X_SERVICE_RES_ID` (`res_id` ASC),
  INDEX `fk_X_TAG_RES_MAP_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_TAG_RES_MAP_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_TAG_RES_MAP_TAG_ID`
    FOREIGN KEY (`tag_id`)
    REFERENCES `x_tag` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_RES_MAP_SERVICE_RES_ID`
    FOREIGN KEY (`res_id`)
    REFERENCES `x_service_resource` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_RES_MAP_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_TAG_RES_MAP_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `x_service_resource_element_value`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `x_service_resource_element_value` ;

CREATE TABLE IF NOT EXISTS `x_service_resource_element_value` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `guid` VARCHAR(512) NOT NULL,
  `create_time` DATETIME NULL,
  `update_time` DATETIME NULL,
  `added_by_id` BIGINT(20) NULL,
  `upd_by_id` BIGINT(20) NULL,
  `res_element_id` BIGINT(20) NOT NULL,
  `value` VARCHAR(512) NOT NULL,
  `sort_order` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_X_RESOURCE_ELEMENT_ID` (`res_element_id` ASC),
  INDEX `fk_X_SERVICE_RES_VAL_MAP_ADDED_BY_ID` (`added_by_id` ASC),
  INDEX `fk_X_SERVICE_RES_VAL_MAP_UPD_BY_ID` (`upd_by_id` ASC),
  CONSTRAINT `fk_X_RESOURCE_ELEMENT_ID`
    FOREIGN KEY (`res_element_id`)
    REFERENCES `x_service_resource_element` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RES_VAL_MAP_ADDED_BY_ID`
    FOREIGN KEY (`added_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT `fk_X_SERVICE_RES_VAL_MAP_UPD_BY_ID`
    FOREIGN KEY (`upd_by_id`)
    REFERENCES `x_portal_user` (`id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT)
ENGINE = InnoDB;


-- ----------------------------------------------------------------
-- ranger database add column in x_service_def and x_service table
-- ----------------------------------------------------------------
alter table x_service_def add column `options` VARCHAR(1024) DEFAULT NULL NULL;
alter table x_policy_item add column `item_type` INT DEFAULT 0 NOT NULL;
alter table x_service add column `tag_service` BIGINT DEFAULT NULL NULL,
                      add column `tag_version` BIGINT DEFAULT 0 NOT NULL,
                      add column `tag_update_time` DATETIME DEFAULT NULL NULL;
