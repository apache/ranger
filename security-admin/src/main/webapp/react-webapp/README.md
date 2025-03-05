<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Ranger React

## Building Ranger Admin UI Package

To build the Ranger Admin UI package, run the following Maven command:
```
mvn clean compile package -DskipTests -DskipJSTests=true
```

## Updating React JS Code to Comply with Checkstyle Requirements

#### Option 1: Using Visual Studio Code

1. Install the **Prettier - Code Formatter** extension from the Visual Studio Code Extensions panel.
2. Go to **Settings > User > Text Editor** and set **Change Default Formatter** to **Prettier - Code Formatter**.
3. Set `prettier.configPath` to the full path of the  [.prettierrc](https://github.com/apache/ranger/blob/master/security-admin/src/main/webapp/react-webapp/.prettierrc ".prettierrc") file in your Ranger project directory. Example:
   ```bash
   /home/user/Workspace/apache/ranger/security-admin/src/main/webapp/react-webapp/.prettierrc
   ```
4. Similarly set `prettier.ignorePath` to the full path of the [.prettierignore](https://github.com/apache/ranger/blob/master/security-admin/src/main/webapp/react-webapp/.prettierignore ".prettierignore") file to ignore specific files and directories from formatting.
Example:
   ```bash
   /home/user/Workspace/apache/ranger/security-admin/src/main/webapp/react-webapp/.prettierignore
   ```
5. Enable **Format On Save** in the editor settings.

#### Option 2: Using Prettier npm Command

1. Go to react-webapp directory
   ```bash
   cd security-admin/src/main/webapp/react-webapp
   ```
2. Install Prettier
   ```bash
   npm install --save-dev prettier
   ```
3. To format a specific file, run:
   ```bash
   npx prettier --write src/App.jsx
   ```
4. To format all files in a directory, run:
   ```bash
   npx prettier --write src/
   ```
5. After running the command `npm install --save-dev prettier` and manually formatting the changed files, please ensure that you do not push the `package.json` and `package-lock.json` files.