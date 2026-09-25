---
title: "Ranger Release Guidelines"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

[keys]: https://dist.apache.org/repos/dist/release/ranger/KEYS
[DockerHub]: https://hub.docker.com/r/apache/ranger
## Introduction

This document provides steps involved in pre and post release of Apache Ranger. [Here](https://www.apache.org/legal/release-policy.html) you can read about the release process in general for an Apache project.

Decisions about releases are made by three groups:

* Release Manager: Does the work of creating the release, signing it, counting votes, announcing the release and so on.
* The Community: Performs the discussion of whether it is the right time to create a release and what that release should contain. The community can also cast non-binding votes on the release.
* PMC: Gives binding votes on the release.

This page describes the procedures that the release manager and voting PMC members take during the release process.

## Prerequisite
The release manager should have a gpg key setup to sign the artifacts. For more details, please see: [https://www.apache.org/dev/new-committers-guide.html#set-up-security-and-pgp-keys](https://www.apache.org/dev/new-committers-guide.html#set-up-security-and-pgp-keys)

#### Setup for first time release managers

#### Generate OpenPPG Key

```bash title="Generate Key and Upload"
# create a key
gpg --gen-key

# If you have multiple keys present, select the key id you want to use, let's say it is - your_gpg_key_id then do:
export CODESIGNINGKEY=your_gpg_key_id

gpg --list-keys ${CODESIGNINGKEY}
# to upload the key to a key server
gpg --keyserver hkp://keyserver.ubuntu.com --send-key ${CODESIGNINGKEY}
```

## Publish your key
The key is supposed to be published together with the release. Please append it to the end of KEYS file: [https://dist.apache.org/repos/dist/release/ranger/KEYS][keys]

```bash title="Publish Key (PMC)"
svn co https://dist.apache.org/repos/dist/release/ranger
cd ranger
gpg --list-sigs $CODESIGNINGKEY >> KEYS
gpg --armor --export $CODESIGNINGKEY >> KEYS

svn commit -m "Adding key of XXXX to the KEYS"
```

!!! note

    In case you are a committer and not a PMC member, you can add your key to the dev `KEYS` file and a PMC can move it to the final destination.

```bash title="Move Key (Committer)"
svn co https://dist.apache.org/repos/dist/dev/ranger
cd ranger
gpg --list-sigs $CODESIGNINGKEY >> KEYS
gpg --armor --export $CODESIGNINGKEY >> KEYS
svn add KEYS
svn commit -m "Adding key of XXXX to the KEYS"
```

## Pre-Vote

### Create a parent Jira for the release
This provides visibility into the progress of the release for the community. Tasks mentioned in this guide like changing snapshot versions, updating the Ranger website, publishing the artifacts, publishing the docker image, etc can be added as subtasks. Here is an example: [RANGER-5098](https://issues.apache.org/jira/browse/RANGER-5098)

### Notify the community in advance of the release
The below details should be included when sending out an email to: `dev@ranger.apache.org`

* The release branch to be used for the release.
* The release branch lockdown date, the branch will be closed for commits after this date. Commits after this date will require approval from PMCs.
* Tentative date for the availability of release-candidate #0, after which voting begins. A minimum of 72 hours needs to pass before the voting can close.
* Tentative release date.

### Branching
A release branch should already be available as a post-release activity from the previous release. All release related changes will go to this branch until the release is complete.

* Ensure that there is no `OPEN` Jira associated with the release.

### Update the versions
```bash title="Update versions"
# Use below command or use IDE to replace "${RANGER_VERSION}-SNAPSHOT" with "${RANGER_VERSION}".
export RANGER_VERSION=2.8.0

mvn versions:set -DnewVersion=${RANGER_VERSION} -DgenerateBackupPoms=false

# Also, manually update versions in:
# - dev-support/ranger-docker/.env
# - docs/pom.xml
# - unixauthnative/pom.xml
# - ranger-trino-plugin-shim/pom.xml
```

### Commit the changes
```bash title="Commit version changes to release branch"
export RANGER_VERSION=2.8.0 # Set to the version of Ranger being released.

git commit -am "RANGER-XXXX: Updated version from ${RANGER_VERSION}-SNAPSHOT to ${RANGER_VERSION}"

git push origin

# for ex: https://github.com/apache/ranger/commit/81f3d2f
```

```bash title="Tag the RC and Push"
export RANGER_RC=0 # Set to the number of the current release candidate, starting at 0. 
git tag -a release-${RANGER_VERSION}-rc${RANGER_RC} -m "Ranger ${RANGER_VERSION}-rc${RANGER_RC} release"

# example: git tag -a release-2.8.0-rc0 -m "Ranger 2.8.0-rc0 release"

# and then push to the release branch like this
git push origin release-${RANGER_VERSION}-rc${RANGER_RC}
```

### Build and Publish Source Artifacts

#### Set up local environment

It is probably best to clone a fresh Ranger repository locally to work on the release, and leave your existing repository intact for dev tasks you may be working on simultaneously.
After cloning, make sure the `apache/ranger` upstream repo is named `origin`.
This is required for release build metadata to be correctly populated.
Assume all following commands are executed from within this repo with your release branch checked out.

```bash
export RANGER_RC=0 # Set to the number of the current release candidate, starting at 0.
export CODESIGNINGKEY=your_gpg_key_id
```

#### Reset the git repository
```bash title="Reset git repo"
git reset --hard
 
git clean -dfx
```

#### Create the release artifacts
```bash title="Build Ranger"
# run with unit tests
mvn clean install -Dmaven.javadoc.skip=true
```

* Verify `LICENSE` and `NOTICE` files for the release are updated based on changes in the release.
* Go through all commits in this particular release and create Release Notes. for example: [Apache Ranger 2.8.0 - Release Notes](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=406620844)
* Also, ensure the fix versions are appropriately added for all the jiras related to the commits.

#### Calculate the checksum and sign the artifacts
```bash title="sign artifact and calculate checksum"
export GPG_TTY=$(tty)
ant -f release-build.xml -Dranger-release-version=${RANGER_VERSION} -Dsigning-key=${CODESIGNINGKEY}

# on successful run, the above command generates 4 files in target:
# - apache-ranger-${RANGER_VERSION}.tar.gz
# - apache-ranger-${RANGER_VERSION}.tar.gz.asc
# - apache-ranger-${RANGER_VERSION}.tar.gz.sha512
# - apache-ranger-${RANGER_VERSION}.tar.gz.sha256
#
# each binary in target/ also gets .asc and .sha512

# verify the signed source tarball and checksum file using below command
cd target
gpg --verify apache-ranger-${RANGER_VERSION}.tar.gz.asc apache-ranger-${RANGER_VERSION}.tar.gz
sha512sum -c apache-ranger-${RANGER_VERSION}.tar.gz.sha512
sha256sum -c apache-ranger-${RANGER_VERSION}.tar.gz.sha256
```

#### Publish build artifacts to dev
Upload the source tarball and binary artifacts into the **same** RC directory, for example `dev/ranger/${RANGER_VERSION}-rc${RANGER_RC}/`:

```text title="publish build artifacts"
${RANGER_VERSION}-rc${RANGER_RC}/
├── apache-ranger-${RANGER_VERSION}.tar.gz (+ .asc, .sha256, .sha512)
├── plugins/
│   ├── atlas/
│   ├── elasticsearch/
│   ├── hbase/
│   ├── hdfs/
│   ├── hive/
│   ├── kafka/
│   ├── knox/
│   ├── kylin/
│   ├── ozone/
│   ├── presto/
│   ├── schema-registry/   (may be .jar)
│   ├── solr/
│   ├── sqoop/
│   ├── storm/
│   ├── trino/             (if built for this release)
│   └── yarn/
├── services/
│   ├── admin/
│   ├── kms/
│   ├── pdp/               (if built for this release)
│   ├── tagsync/
│   └── usersync/
└── tools/
    ├── migration-util/
    ├── ranger-tools/
    ├── sample-client/
    └── solr_audit_conf/
```

Each leaf directory holds one artifact plus `.asc` and `.sha512` (binaries do not use `.sha256`).

```bash title="publish build artifacts"
svn co https://dist.apache.org/repos/dist/dev/ranger ranger-dev

mkdir ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}
# 1) source tarball
cp target/apache-ranger-${RANGER_VERSION}.tar.gz ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/
cp target/apache-ranger-${RANGER_VERSION}.tar.gz.asc ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/
cp target/apache-ranger-${RANGER_VERSION}.tar.gz.sha256 ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/
cp target/apache-ranger-${RANGER_VERSION}.tar.gz.sha512 ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/

# 2) binary artifacts — copy each ranger-${RANGER_VERSION}-* file from target/
#    into the plugins/, services/, or tools/ subtree shown above.
#    Example: target/ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz*
#             -> ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/plugins/hdfs/

svn add ${RANGER_VERSION}-rc${RANGER_RC}
svn commit -m "RANGER-XXXX: Upload ${RANGER_VERSION}-rc${RANGER_RC} source and binary artifacts" # requires ASF authentication 
```

## Vote

### Send the voting mail as described below

- Send release voting request to `dev@ranger.apache.org` and `private@ranger.apache.org` with the subject line

    ```
    [VOTE] Release Apache Ranger ${RANGER_VERSION} ${RANGER_RC}
    ```

- Include the following in the email:
    - Link to a Jira query showing all resolved issues for this release. Something like [this](https://issues.apache.org/jira/issues/?jql=project%3DRANGER%20and%20fixVersion%20%20%3D%202.6.0%20and%20status%20in%20(Resolved%2C%20Closed)%20ORDER%20BY%20key%20desc).
    - Link to the release candidate tag on Github.
    - Location of the source and binary tarballs. This link will look something like [https://dist.apache.org/repos/dist/dev/ranger/2.8.0-rc1/](https://dist.apache.org/repos/dist/dev/ranger/2.8.0-rc1/)
    - Link to the public key used to sign the artifacts. This should always be in the KEYS file and you can just link to that: [https://dist.apache.org/repos/dist/release/ranger/KEYS][keys]
- The vote will be open for at least 72 hours or until necessary votes are reached.

    ```
    [ ] +1 approve
    [ ] +0 no opinion
    [ ] -1 disapprove (and reason why)
    ```

- Review [https://www.apache.org/legal/release-policy.html#release-approval](https://www.apache.org/legal/release-policy.html#release-approval) for the ASF wide release voting policy.

    !!! note

        Note what is required of binding voters, and that binding votes can only come from PMC members. Check [https://people.apache.org/committer-index.html](https://people.apache.org/committer-index.html), users whose group membership includes *ranger-pmc* can cast binding votes.

- If VOTE did not go through,
    - Apply fixes to the release branch and repeat the steps starting from **tagging the commit for the release candidate** with the `$RANGER_RC` variable incremented by 1 for all steps.
- Once voting is finished, send an email summarizing the results to `dev@ranger.apache.org` and `private@ranger.apache.org` with subject like

    ```
    [RESULT] [VOTE] Apache Ranger ${RANGER_VERSION} ${RANGER_RC}
    ```

    Include names of all PMC members, followed by committers/contributors who casted their votes. Here is a reference link: [https://lists.apache.org/thread/sonr9mmjv8ot9kzwh66royv0pblnn41c](https://lists.apache.org/thread/sonr9mmjv8ot9kzwh66royv0pblnn41c)

## After-Vote

### Publish the source artifacts to dist.apache.org

You should commit the artifacts to the SVN repository. If you are not a PMC member you can commit it to the dev ranger first and ask a PMC for the final move.

PMC members can move it to the final location:

```bash title="Move"
svn co https://dist.apache.org/repos/dist/dev/ranger ranger-dev

svn co https://dist.apache.org/repos/dist/release/ranger ranger-release

mkdir ranger-release/${RANGER_VERSION}

cp -R ranger-dev/${RANGER_VERSION}-rc${RANGER_RC}/* ranger-release/${RANGER_VERSION} # copy release artifacts from dev to release

cd ranger-release

svn add ${RANGER_VERSION}

svn commit -m "Uploading Apache Ranger ${RANGER_VERSION} release source and binary artifacts" ${RANGER_VERSION}
```

The source tarball should have `.asc`, `.sha512`, and `.sha256` files, so a total of 4 files. Each binary artifact should have `.asc` and `.sha512`.

### Publish the source artifacts to Maven Central
1. Setup `~/.m2/settings-security.xml` per guidelines at [https://maven.apache.org/guides/mini/guide-encryption.html](https://maven.apache.org/guides/mini/guide-encryption.html).
2. Encrypt your Apache account password using above guidelines, and enter it in `~/.m2/settings.xml` in the following entry:

    ```xml title="Update settings.xml"
    <server>
         <id>apache.staging.https</id>
         <username>username</username>
         <password>encrypted_password</password>
    </server>
    ```

3. Run the following:

    ```bash title="checkout and deploy"
    # checkout the relevant git tag

    git checkout release-ranger-${RANGER_VERSION}
    # eg: git checkout release-ranger-2.8.0

    # deploy the release
    mvn clean deploy -Papache-release -DskipTests -Dmaven.javadoc.skip=true
    ```

    If the above command results in 401, you may try with plain passwords instead in `~/.m2/settings.xml`.

4. Go to [https://repository.apache.org/](https://repository.apache.org/) and log in using your Apache account.
5. Click on "Staging Repositories" on the left-hand side.
6. Select the entry that starts with orgapacheranger and click on "close".
7. Verify via the URL that should appear after refresh that the artifacts look as expected.
8. After approval, click on "release".

### Publish build artifacts
```bash title="build ranger release and push artifacts to svn"
# build ranger from the release branch

# create parent directory before build
RELEASE_DIR=/tmp/release-${RANGER_VERSION}
mkdir -p ${RELEASE_DIR} && cd ${RELEASE_DIR}

git clone https://github.com/apache/ranger.git && cd ranger

git checkout release-ranger-${RANGER_VERSION}

# after successful build, artifacts should be present in target
mvn clean package -DskipTests

# checkout svn repo
cd ~
svn co https://dist.apache.org/repos/dist/dev/ranger ranger-dev && cd ranger-dev
cd ${RANGER_VERSION}-rc${RANGER_RC}
cp ${RELEASE_DIR}/ranger/target/ranger-* .

# generate signature and checksums for all
for file in `find . -name "ranger-*"`
do
  gpg --armor --output ${file}.asc --detach-sig ${file} && sha512sum ${file} > ${file}.sha512
done

svn add ranger-*
svn commit -m "upload build artifacts for ${RANGER_RELEASE} release"

# PMC Members may selectively move these artifacts to https://dist.apache.org/repos/dist/release/ranger/${RANGER_RELEASE} under respective directories
```

### Add the final git tag and push it
```bash title="Add final release tag"
git checkout "release-${RANGER_VERSION}-rc${RANGER_RC}"
 
git tag -a "release-ranger-${RANGER_VERSION}" -m "Apache Ranger $RANGER_VERSION"
 
git push origin "release-ranger-${RANGER_VERSION}"
```

### Create a sub-page in Confluence
Add a sub-page under [Release Folders](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=217390192) for this release and add links for the following:

* Link to the release notes
* Link to the release artifacts
* Link to the release tag

Something like [this](https://cwiki.apache.org/confluence/display/RANGER/2.6.0+release+-+Apache+Ranger)

### Update the Ranger website

The live Ranger website (the Maven site built from `docs/src/site`: download page, Releases menu) is **not** auto-deployed from GitHub. It is published from SVN:

Use **two steps**:

#### Step 1 — GitHub PR to `master` (source)

Create a a PR targeted for master branch to update the docs with the new release:

| File                              | Change                                                                 |
|:----------------------------------|:-----------------------------------------------------------------------|
| `docs/src/site/xdoc/download.xml` | Add `${RANGER_VERSION}` as **Current Stable**; demote previous version |
| `docs/src/site/site.xml`          | Add `${RANGER_VERSION}` release notes link in **Releases** menu        |

Example PRs: [#532](https://github.com/apache/ranger/pull/532) (2.6.0), [#1136](https://github.com/apache/ranger/pull/1136) (2.9.0)

Merge the PR before Step 2.

#### Step 2 — SVN publish (live site)

**Who:** Release Manager (any committer with Apache LDAP + SVN write access)

##### Option Minimal update (download + Releases menu only)

```shell title="Minimal update (download + Releases menu only)"
svn co https://svn.apache.org/repos/asf/ranger/site/trunk /tmp/ranger-site
# Update download.html: set Current Stable → ${RANGER_VERSION}
#   (tarball, PGP, digest links under downloads.apache.org/ranger/${RANGER_VERSION}/)
# Update Releases menu (top nav + footer) on all main pages — 11 files:
#   download.html, index.html, faq.html, blogs.html, quick_start_guide.html,
#   team-list.html, project-summary.html, project-info.html, license.html,
#   issue-tracking.html, mail-lists.html
#   (add ${RANGER_VERSION} release-notes link above the previous release)
cd /tmp/ranger-site
svn diff
svn commit -m "RANGER-XXXX: Publish Apache Ranger ${RANGER_VERSION} website"
```

#### Verify

* Download page of the live site → **Current Stable release - Apache Ranger ${RANGER_VERSION}**
* **Releases** menu of the live site lists **${RANGER_VERSION}**

In this documentation, add the new version to [Releases](../release-notes/index.md) and [Download and verify](../release-notes/download.md).

### Publish docker images for the release
Run this GitHub Actions workflow: [https://github.com/apache/ranger-tools/actions/workflows/build-and-tag-ranger-image.yaml](https://github.com/apache/ranger-tools/actions/workflows/build-and-tag-ranger-image.yaml) (requires PMC level permissions)

Use this command to generate a unique 11 character token:

```bash
code=$(uuidgen | tr A-Z a-z | cut -c 1-11)
```

and then pass this to the state param here:

```
https://oauth.apache.org/auth?redirect_uri=https://ranger.apache.org&state=<code>
```

On successful authentication with ASF, it generates an OAuth token on the redirect page (the Ranger website) which should be used when triggering the workflow.

On successful workflow run, the images - ranger, ranger-db, ranger-solr are pushed to [DockerHub] and GitHub Container registries.

#### Manual alternative

If the workflow cannot be used, build the following docker images:

* ranger
* ranger-db
* ranger-solr
* ranger-zk

with the release checked out and upload them to [DockerHub].
Instructions to build the images can be found [here](https://github.com/apache/ranger/blob/master/dev-support/ranger-docker/README.md).
```bash title="tag and push docker images"
# tag the images
docker tag ranger:latest apache/ranger:${RANGER_VERSION}
docker tag ranger-db:latest apache/ranger-db:${RANGER_VERSION}
docker tag ranger-solr:latest apache/ranger-solr:${RANGER_VERSION}
docker tag ranger-zk:latest apache/ranger-zk:${RANGER_VERSION}

# do docker login
docker login

# push the images
docker push apache/ranger:${RANGER_VERSION}
docker push apache/ranger-db:${RANGER_VERSION}
docker push apache/ranger-solr:${RANGER_VERSION}
docker push apache/ranger-zk:${RANGER_VERSION}
```

### Send an announcement mail

to `dev@ranger.apache.org`, `user@ranger.apache.org`, `announce@apache.org`. Something like this: [https://lists.apache.org/thread/4ssdwwpdcd8381k09otjfsydb47z1ygm](https://lists.apache.org/thread/4ssdwwpdcd8381k09otjfsydb47z1ygm)

```
Subject: [ANNOUNCE] Apache Ranger ${RANGER_VERSION}
```

!!! note

    Only PMC members can send the email to `announce@apache.org`.

- Include the following in the email:
    - Download link: the [download](../release-notes/download.md) page
    - Release notes: something like - [Apache Ranger 2.8.0 - Release Notes](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=406620844)
    - When downloading binaries from the site, please remember to verify the downloads using signatures at: [https://www.apache.org/dist/ranger/KEYS](https://www.apache.org/dist/ranger/KEYS).

### Branching

Create a new release branch, for ex: ranger-2.9 from ranger-2.8. In this release branch, do the following and commit it.

```bash title="Update to SNAPSHOT version and Push"
NEXT_RANGER_VERSION=2.9.0-SNAPSHOT
mvn versions:set -DnewVersion=${NEXT_RANGER_VERSION}

git commit -am "RANGER-XXXX: Updated version from ${RANGER_VERSION} to ${NEXT_RANGER_VERSION}"

# Also, manually update versions in:
# - dev-support/ranger-docker/.env
# - docs/pom.xml

git push origin
```

Now, update the previous release branch with newer SNAPSHOT version and commit it, something like this:

```bash title="Update to SNAPSHOT version and Push"
NEXT_RANGER_VERSION=2.8.1-SNAPSHOT
mvn versions:set -DnewVersion=${NEXT_RANGER_VERSION}

git commit -am "RANGER-XXXX: Updated version from ${RANGER_VERSION} to ${NEXT_RANGER_VERSION}"

# Also, manually update versions in:
# - dev-support/ranger-docker/.env
# - docs/pom.xml

git push origin
```

### Other Tasks
- In Apache JIRA admin, mark the release as complete and create a next version for tracking the changes to the next (major|minor) version
- Update release data in [https://reporter.apache.org/?ranger](https://reporter.apache.org/?ranger).

    !!! note

        Only PMC members can do this step.

- If the release resolved any CVE,
    - please update [Vulnerabilities Found](./cve-list.md) ([https://cwiki.apache.org/confluence/display/RANGER/Vulnerabilities+found+in+Ranger](https://cwiki.apache.org/confluence/display/RANGER/Vulnerabilities+found+in+Ranger))
    - send notification to
        - `security@apache.org`
        - `oss-security@lists.openwall.com`
        - `dev@ranger.apache.org`
        - `user@ranger.apache.org`
        - `private@ranger.apache.org`
    - Follow [https://www.apache.org/security/committers.html](https://www.apache.org/security/committers.html) for publishing the CVE
