---
order: 4
---

# BitSail Release Guide

English | [简体中文](../../zh/community/release_guide.md)

-----

## Procedure to submit a pull request

SOP to submit a new commit

1. Create a Github issue or claim an existing issue
2. Describe what you would like to do in the issue description. 
3. Include the issue number in the commit message. The format follows below.

```Plain
[BitSail#${IssueNumber}][${Module}] Description
[BitSail#1234][Connector] Improve reader split algorithm to Kudu source connector

//For Minor change
[Minor] Description
```

4. List of module. Chose the most related one if your changes affect multiple modules. e.g. If you are adding a feature to kafka connector and end up modifying code in common, components and cores, you should still use the [Connector] as module name.

```Plain
[Common] bitsail-common
[Core] base client component cores
[Connector] all connector related changes
[Doc] documentation or java doc changes
[Build] build, dependency changes
```

## Procedure to release

![img](../../images/community/release_guide/release_procedure.png)

### 1. Decide to release

Because we don't have many users subscribed to the mailing list for now. Using Github issue to discuss release related topic should have better visibility. 

We could start a new discussion on Github with topics like

`0.1.0` Release Discussion

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based decision of the entire community.

Anybody can propose a release on the Github issue, giving a solid argument and nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any objections should be resolved by consensus before starting the release.

### 2. Prepare for the relase

A. Triage release-blocking issues

B. Review and update documentation

C. Cross team testing

D. Review Release Notes

E. Verify build and tests

F. Create a release branch

G. Bump the version of the master

### 3. Build a release candidate

Since we don't have a maven central access for now, we will build a release candidate on github and let other users test it.

A. Add git release tag

B. Publish on Github for the public to download

### 4. Vote on the release candidate

Once the release branch is ready and release candidate is available on Github. The release manager will ask other committers to test the release candidate and start a vote on corresponding Github Issue. We need 3 blinding votes from PMC members at least. 

### 5. Fix issue

Any issues identified during the community review and vote should be fixed in this step.

Code changes should be proposed as standard pull requests to the master branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

### 6. Finalize the release

Once the release candidate passes the voting, we could finalize the release.

A. Change the release branch version from `x.x.x-rc1` to `x.x.x`. e.g. `0.1.0-rc1` to `0.1.0`

B. `git commit -am "[MINOR] Update release version to reflect published version  ${RELEASE_VERSION}"`

C. Push to the release branch

D. Resolve related Github issues

E. Create a new Github release, off the release version tag, you pushed before

### 7. Promote the release

24 hours after we publish the release, promote the release on all the community channels, including WeChat, slack, mailing list.

### Reference:

Flink release guide: [Creating a Flink Release](https://cwiki.apache.org/confluence/display/FLINK/Creating+a+Flink+Release)

Hudi release guide: [Apache Hudi Release Guide](https://cwiki.apache.org/confluence/display/HUDI/Apache+Hudi+-+Release+Guide)