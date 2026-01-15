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
[Review Board]: https://reviews.apache.org/
[Slack]: https://the-asf.slack.com/archives/C4SC5NXAA
[Dev List]: mailto:dev-subscribe@ranger.apache.org
# Contributing

In this page, you will find some guidelines on contributing to Apache Ranger.

If you are thinking of contributing but first would like to discuss the change you wish to make, we welcome you to
raise a [Jira Issue]. You can also subscribe to the [Dev List] and join us on [Slack]
to connect with the community.

The Ranger Project is hosted on GitHub at <https://github.com/apache/ranger>.

## Pull Request <small>recommended</small>

The Ranger community prefers to receive contributions as [Github pull requests][github-pr-docs].

[View open pull requests][ranger-prs]

When you are ready to submit your pull request, please keep the following in mind:

* PRs should be associated with a [Jira Issue]
* PRs should include a clear and descriptive title and summary of the change
* Please ensure that your code adheres to the existing coding style
* Please ensure that your code is well tested
* Please ensure that your code is well documented


## Review Board <small>legacy</small>

The [Review Board] may be used for Ranger code reviews as well.

To submit a patch for review, please follow these steps:

- Create a [Jira Issue] for the change you wish to make.
- Create a patch file using `git format-patch` or `git diff > my_patch.patch`.
- Upload the patch to [Review Board] and associate it with the Jira issue you created earlier.
- Request a review from the Ranger committers.
- Address any feedback you receive and update the patch as necessary.
- Once your patch has been approved, a committer will merge it into the main codebase.
- Close the associated Jira issue.
