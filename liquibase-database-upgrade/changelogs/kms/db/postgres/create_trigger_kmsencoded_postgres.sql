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

--liquibase formatted sql

-- Create a trigger function
CREATE OR REPLACE FUNCTION copy_data_trigger_function()
RETURNS TRIGGER AS
'
BEGIN
     -- If column_a is updated and is not null, update column_b
     IF NEW.kms_encoded IS NOT NULL AND NEW.kms_encoded_value IS NULL THEN
          NEW.kms_encoded_value := NEW.kms_encoded;
     END IF;
     -- If column_b is updated and is not null, update column_a
     IF NEW.kms_encoded_value IS NOT NULL AND NEW.kms_encoded is NULL THEN
            NEW.kms_encoded := NEW.kms_encoded_value;
     END IF;
     RETURN NEW;
END;
'
LANGUAGE plpgsql;

-- Create a trigger
CREATE TRIGGER copy_data_trigger
BEFORE INSERT OR UPDATE ON ranger_keystore
FOR EACH ROW
EXECUTE PROCEDURE copy_data_trigger_function();