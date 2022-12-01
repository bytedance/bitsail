# Contributor Guide

BitSail community welcomes contributions from anyone!

## How To Get Involved

- If you meet any issue when using BitSail, submit a GitHub issue to report.
- If you want a new feature, submit a GitHub issue to request the feature.
- Ask any question on the Slack channel, or help to answer other user's question.
- Claim an issue to fix bug or develop new features.
- Improve documentation, code comments, typos and test coverages.
- Start a discussion on mailing list if you have a large feature to bring in.

## Open a GitHub Issue
- Search for known issues to check whether there have been an existing topic
- If Yes, you can participate in the discussion
- If Not, create a new issue by yourself according to the doc [Create a New Issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue)

## Environment Setup
Link to [environment setup](env_setup.md)

## Code Style

We generally follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)

## IDE Setup

We are using Intellij as an example for this set up.

### CheckStyle

Install the checkstyle plugin on your IDE.

![](../images/checkstyle_plugin.png)

We define check style in [tools/maven/checkstyle.xml](https://github.com/bytedance/bitsail/blob/master/tools/maven/checkstyle.xml).
When developing in IDE, you can import this checkstyle configuration file.

![](../images/set_checkstyle.png)

After importing, run command `mvn checkstyle:check` to see if checkstyle satisfied.

### Install lombok

We use project [lombok](https://projectlombok.org/). So please install this plugin as well.

### Add License Header
All the source files are required to add License header. Please add [Apache License header](https://www.apache.org/legal/src-headers#headers)
to your IDE Copyright settings. After this step, IDE will add license to new source files automatically.

![](../images/license_header.png)

## Developing Tips
### Maven Dependency Enforcement
We check the overlapping packages during the build process. When you see a conflict error during the build process,
please exclude the conflicting packages from the pom file.

## Submit a Pull Request
If it is the first time to submit a pull request, you can read this doc [About Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests)

- [Fork ***BitSail*** Repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
- Checkout a new branch in your fork repo
- Patch you code, including necessary test cases & coding-style check
- Commit changes to the branch and push to the fork repo
- Create a pull request to the ***BitSail*** repo

## Ask for a code review
After you have your pull request ready, with all the items from the pull request checklist being completed. 
Tag a committer to review you pull request.
