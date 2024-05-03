<<<<<<< HEAD
<!--
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
# Apache Ranger

[![License](https://img.shields.io/:license-Apache%202-green.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/apache-ranger?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/apache-ranger/)
[![Documentation](https://img.shields.io/badge/docs-apache.org-blue.svg)](https://ranger.apache.org)
[![Wiki](https://img.shields.io/badge/ranger-wiki-orange)](https://cwiki.apache.org/confluence/display/RANGER/Index)


#### NOTE
Apache Ranger allows contributions via pull requests (PRs) on GitHub.  
Alternatively, use [this](https://reviews.apache.org) to submit changes for review using the Review Board.
Also create a [ranger jira](https://issues.apache.org/jira/browse/RANGER) to go along with the review and mention it in the review board review.


## Building Ranger in Docker (Sandbox Install)

Ranger is built using [Apache Maven](https://maven.apache.org/). To run Ranger:

1. Check out the code from GIT [repository](https://github.com/apache/ranger.git)

2. Ensure that docker & docker-compose is installed and running on your system.

3. Ensure that JDK 1.8+ is installed on your system.

4. Ensure that Apache Maven is installed on your system.

5. Run the following command to build & run Ranger from Docker

   `./ranger_in_docker up`

6. After successful completion of the above command, you should be able to view Ranger Admin Console by using URL:
   ```
    http://<hostname-of-system>:6080/

    UserName: admin
    Password: rangerR0cks!
   ```

## Regular Build Process

1. Check out the code from GIT repository

2. On the root folder, please execute the following Maven command:

   `mvn clean compile package install`

   `mvn eclipse:eclipse`

   Ranger Admin UI tests depend on PhantomJS. If the build fails with npm or Karma errors you can either:
   - install PhantomJS dependencies for your platform (bzip2 and fontconfig)
   - skip JavaScript test execution: mvn -DskipJSTests ...

3. After the above build command execution, you should see the following TAR files in the target folder:
   ```
   ranger-<version>-admin.tar.gz
   ranger-<version>-atlas-plugin.tar.gz
   ranger-<version>-hbase-plugin.tar.gz
   ranger-<version>-hdfs-plugin.tar.gz
   ranger-<version>-hive-plugin.tar.gz
   ranger-<version>-kafka-plugin.tar.gz
   ranger-<version>-kms.tar.gz
   ranger-<version>-knox-plugin.tar.gz
   ranger-<version>-migration-util.tar.gz
   ranger-<version>-ranger-tools.tar.gz
   ranger-<version>-solr-plugin.tar.gz
   ranger-<version>-sqoop-plugin.tar.gz
   ranger-<version>-src.tar.gz
   ranger-<version>-storm-plugin.tar.gz
   ranger-<version>-tagsync.tar.gz
   ranger-<version>-usersync.tar.gz
   ranger-<version>-yarn-plugin.tar.gz
   ranger-<version>-kylin-plugin.tar.gz
   ranger-<version>-elasticsearch-plugin.tar.gz
   ```

## Importing Apache Ranger Project into Eclipse

1. Create an Eclipse workspace called 'ranger'

2. Import maven project from the root directory where ranger source code is downloaded (and build)


## Deployment Process


### Installation Host Information
1. Ranger Admin Tool Component  (ranger-<version-number>-admin.tar.gz) should be installed on a host where Policy Admin Tool web application runs on port 6080 (default).
2. Ranger User Synchronization Component (ranger-<version-number>-usersync.tar.gz) should be installed on a host to synchronize the external user/group information into Ranger database via Ranger Admin Tool.
3. Ranger Component plugin should be installed on the component boxes:
   - HDFS Plugin needs to be installed on Name Node hosts.
   - Hive Plugin needs to be installed on HiveServer2 hosts.
   - HBase Plugin needs to be installed on both Master and Regional Server nodes.
   - Knox Plugin needs to be installed on Knox gateway host.
   - Storm Plugin needs to be installed on Storm hosts.
   - Kafka/Solr Plugin needs to be installed on their respective component hosts.
   - YARN plugin needs to be installed on YARN Resource Manager hosts.
   - Sqoop plugin needs to be installed on Sqoop2 hosts.
   - Kylin plugin needs to be installed on Kylin hosts.
   - Elasticsearch plugin needs to be installed on Elasticsearch hosts.

### Installation Process

1. Download the tar.gz file into a temporary folder in the box where it needs to be installed.

2. Expand the tar.gz file into /usr/lib/ranger/ folder

3. Go to the component name under the expanded folder (e.g. /usr/lib/ranger/ranger-<version-number>-admin/)

4. Modify the install.properties file with appropriate variables 

5. - If the module has setup.sh, execute ./setup.sh
   - If the install.sh file does not exists, execute ./enable-<component>-plugin.sh

=======
# ranger



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://git.crptech.ru/anst/external/ranger.git
git branch -M master
git push -uf origin master
```

## Integrate with your tools

- [ ] [Set up project integrations](https://git.crptech.ru/anst/external/ranger/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
>>>>>>> 0d1b29299ae1400737426d80f8a2b185c8544ae8
