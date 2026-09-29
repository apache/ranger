---
title: "Contribute"
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
[ranger-prs]: https://github.com/apache/ranger/pulls
[github-pr-docs]: https://help.github.com/articles/about-pull-requests/
[Jira Issue]: https://issues.apache.org/jira/browse/RANGER
[Slack]: https://the-asf.slack.com/archives/C4SC5NXAA
[Dev List]: mailto:dev-subscribe@ranger.apache.org
[unassigned-jiras]: https://issues.apache.org/jira/issues/?jql=project%20%3D%20RANGER%20AND%20assignee%20is%20EMPTY%20ORDER%20BY%20created%20DESC
[jira-account]: https://selfserve.apache.org/jira-account.html
[ci-github]: https://github.com/apache/ranger/actions
[ci-jenkins]: https://ci-builds.apache.org/job/Ranger
# Contributing

In this page, you will find some guidelines on contributing to Apache Ranger.

If you are thinking of contributing but first would like to discuss the change you wish to make, we welcome you to
raise a [Jira Issue]. You can also subscribe to the [Dev List] and join us on [Slack]
to connect with the community.

The Ranger Project is hosted on GitHub at <https://github.com/apache/ranger>.

Contributions are not limited to code. Bug reports with clear reproduction steps, documentation, answers on the
user mailing list, release testing and votes all add value and are how many committers started. See
[Community](community.md) for the channels and how the project makes decisions.

## Getting started

### Accounts you need

- A GitHub account, to fork the repository and open pull requests.
- An Apache JIRA account, to file and be assigned issues. If you are new to Apache, request one from the
  [self-service JIRA account page][jira-account].
- A subscription to the [Dev List], where design discussions and release votes happen.

### Get the code

Fork [apache/ranger](https://github.com/apache/ranger) on GitHub (one fork is enough for all your work), then
clone your fork and add the Apache repository as `upstream`:

```bash
git clone https://github.com/<your-github-id>/ranger.git
cd ranger
git remote add upstream https://github.com/apache/ranger.git
git remote set-url --push upstream disallowed   # optional: prevents an accidental push to apache master
git remote -v
git fetch upstream
```

The canonical repository is also served by the ASF at `https://gitbox.apache.org/repos/asf/ranger.git`; GitHub is a
synchronized mirror and is where reviews take place.

### Build and test

Ranger builds with Apache Maven and requires JDK 17 (`java.version.required` in the root `pom.xml`):

```bash
mvn clean install                # full build with unit tests
mvn clean package -DskipTests    # faster: skip unit tests
mvn -T 8 clean verify            # what CI runs; includes checkstyle and spotbugs
```

Build artifacts (`ranger-<version>-admin.tar.gz`, `ranger-<version>-<plugin>-plugin.tar.gz`, ...) land in `target/`.
To try your change end to end, `./ranger_in_docker up` builds and starts Ranger Admin and its dependencies in
Docker.

### Find something to work on

1. Log in to the [Apache Ranger JIRA project][Jira Issue].
2. Look through the [unassigned issues][unassigned-jiras] and pick one that interests you, or file a new issue
   describing the bug or feature.
3. Send a note to `dev@ranger.apache.org` asking a PMC member to assign the issue to you.
4. For anything larger than a bug fix, describe the design on the JIRA (or the dev list) before writing a lot of
   code, so reviewers can give early feedback.

## Pull Request <small>recommended</small>

The Ranger community prefers to receive contributions as [Github pull requests][github-pr-docs].

[View open pull requests][ranger-prs]

When you are ready to submit your pull request, please keep the following in mind:

* PRs should be associated with a [Jira Issue]
* PRs should include a clear and descriptive title and summary of the change
* Please ensure that your code adheres to the [Code Style Guide](java-code-style.md)
* Please ensure that your code is well tested
* Please ensure that your code is well documented

### Workflow

1. Configure git with your name and email so commits carry your credit:

    ```bash
    git config user.name  "Your Name"
    git config user.email "you@example.com"
    ```

2. Create a branch for the issue, tracking `upstream/master`:

    ```bash
    git checkout -b RANGER-XXXX --track upstream/master
    ```

3. Make your change and commit it locally. The commit message starts with the JIRA id:

    ```bash
    git add <modified|added|deleted files>
    git commit -m "RANGER-XXXX: <description of the change>"
    ```

4. Push the branch to your fork. The same command pushes later commits, and an open PR picks them up
   automatically:

    ```bash
    git push origin HEAD:RANGER-XXXX
    ```

    `git status` shows whether your branch tracks the upstream one.

5. Open the pull request on GitHub. Title it `RANGER-XXXX: <summary>` (the same text as the commit message), add
   existing committers as reviewers and add yourself as assignee.
6. Fill in the PR template: what changes are proposed, and how the patch was tested (unit tests, manual tests;
   attach a screenshot for UI changes).
7. Link the PR from the JIRA issue and set the fix version if you know it.
8. Respond to review comments by pushing additional commits to the same branch. A committer will squash-merge
   the PR when it is approved (`squash` is the only merge method enabled for the repository), so you do not need
   to squash yourself.
9. After the merge, the JIRA is updated with the commit link and resolved.

The PR template is in
[`.github/pull_request_template.md`](https://github.com/apache/ranger/blob/master/.github/pull_request_template.md).

### Continuous integration

Every push and pull request runs the `CI` GitHub Actions workflow
([`.github/workflows/ci.yml`](https://github.com/apache/ranger/blob/master/.github/workflows/ci.yml)). It:

- builds the project on JDK 17 with `mvn -T 8 clean verify`, which runs unit tests, checkstyle and spotbugs;
- collects JaCoCo code coverage;
- builds the Docker images for Ranger services and plugins from the build output and brings the containers up
  to check that they start.

Results are visible on the [Actions tab][ci-github]. Fix a red build before asking for review. The project also has
a Jenkins job on the [ASF CI][ci-jenkins].

### Keeping your branch up to date

When `upstream/master` moves ahead of your branch, bring in the changes and resolve any conflicts:

=== "Merge"

    ```bash
    git fetch upstream
    git merge upstream/master
    ```

=== "Rebase"

    ```bash
    git fetch upstream
    git rebase upstream/master
    git push --force-with-lease origin HEAD:RANGER-XXXX
    ```

Because PRs are squash-merged, either approach is acceptable; rebase gives reviewers a cleaner diff.

### Reviewing a pull request locally

- With the GitHub CLI: `gh pr checkout <number>`, then `git pull` when the PR is updated.
- Without it, append `.diff` or `.patch` to the PR URL to get a plain diff or a `git am`-able patch, for example
  `https://github.com/apache/ranger/pull/210.patch`.

## Voting on a release

Release votes take place on the [Dev List]; you need to be subscribed to take part. Anyone may test a release
candidate and vote (only PMC votes are binding). When a release manager sends a `[VOTE]` mail:

1. Download the source artifact, its `.asc` signature and the `.sha256`/`.sha512` checksums from the
   `dist.apache.org/repos/dist/dev/ranger/<version>-<rc>` location given in the mail, plus the project
   [KEYS](https://dist.apache.org/repos/dist/release/ranger/KEYS) file.
2. Verify the signature and checksums.
3. Build the source with tests and, ideally, run it (for example with `./ranger_in_docker up`).
4. Reply to the vote thread with `+1`, `0` or `-1` and what you tested, following the
   [Apache voting process](https://www.apache.org/foundation/voting.html).

```bash
RANGER_RELEASE_DIR="${HOME}/ranger-validation"
RANGER_RELEASE="2.9.0"
RANGER_RC="rc1"

RANGER_DOWNLOAD_URL_PREFIX="https://dist.apache.org/repos/dist/dev/ranger/${RANGER_RELEASE}-${RANGER_RC}"
RANGER_RELEASE_SOURCE="apache-ranger-${RANGER_RELEASE}.tar.gz"

mkdir -p "${RANGER_RELEASE_DIR}" && cd "${RANGER_RELEASE_DIR}"
for f in "" .asc .sha256 .sha512; do
  curl -o "${RANGER_RELEASE_SOURCE}${f}" "${RANGER_DOWNLOAD_URL_PREFIX}/${RANGER_RELEASE_SOURCE}${f}"
done
curl -o KEYS https://dist.apache.org/repos/dist/release/ranger/KEYS

gpg --import KEYS
gpg --verify "${RANGER_RELEASE_SOURCE}.asc" "${RANGER_RELEASE_SOURCE}"
shasum -a 256 -c "${RANGER_RELEASE_SOURCE}.sha256"
shasum -a 512 -c "${RANGER_RELEASE_SOURCE}.sha512"

tar xf "${RANGER_RELEASE_SOURCE}"
cd "apache-ranger-${RANGER_RELEASE}"
mvn -Pall -DskipTests=false clean compile package install
```

!!! note
    Older release candidates published the checksum in `gpg --print-md` format rather than the `shasum -c`
    format; if the check fails, compare the digest value by eye. The release manager's side of the process is
    described in [Release process](release-process.md); verifying a published release is covered in
    [Download](../release-notes/download.md).

## For committers

- Merge PRs with the **Squash and merge** button; merge commits and rebase merges are disabled in
  `.asf.yaml`. Keep the `RANGER-XXXX: ...` title as the commit subject and make sure the contributor is the
  author.
- `master` is a protected branch; all changes go through a PR.
- Resolve the JIRA with the fix version and the commit link after merging.
- Commit notifications go to `commits@ranger.apache.org`.
- Committers and PMC members can obtain a free JetBrains IntelliJ IDEA license for open-source development by
  applying with their `@apache.org` address via the
  [JetBrains form for Apache committers](https://www.jetbrains.com/shop/eform/apache?product=ALL).

## Further reading

- [Community](community.md): mailing lists, Slack, JIRA, how the project is run.
- [Code Style Guide](java-code-style.md)
- [Release process](release-process.md)
- cwiki: [Want to contribute to Apache Ranger?](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=55151244),
  [Working with PRs on GitHub](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240885202)
