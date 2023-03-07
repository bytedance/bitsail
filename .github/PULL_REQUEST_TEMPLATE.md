### Prepare a Pull Request

- Title: [BitSail-XYZ][XxxType] Title of the pull request

- Fixes: #XYZ

> **Please change the PR title and the related issue number refer to the following description.**
>
> The above *XYZ* must be replaced with the actual [GitHub Issue](https://github.com/bytedance/bitsail/issues) number, indicating that the current PR is used to solve the problem described in this Issue.
>
> If you don't find an Issue that matches your changes, please [Create Issue](https://github.com/bytedance/bitsail/issues/new/choose) first, then commit this PR.
>
> For more info about the contributing guide, see: [Contributing Guide](https://github.com/bytedance/bitsail/blob/master/website/en/community/contribute.md)

### Motivation

*Describe what this PR does and what problems you're trying to solve.*

### Modifications

*Describe the modifications you've done.*

### Verifying this change

**Please pick either of the following options.**

- [ ] This change is a trivial rework/code cleanup without any test coverage.

- [ ] This change is already covered by existing tests, such as:
  *(please describe tests, example:)*
  - com.bytedance.bitsail.core.EngineTest#testRunEngine

- [ ] This change added tests and can be verified as follows:

  *(example:)*
  - Added unit tests for parsing of configs
  - Optimized integration tests for recovery after task failure

### Documentation

- Does this pull request introduce a new feature? (yes / no)
- If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
- If a feature is not applicable for documentation, explain why?
- If a feature is not documented yet in this PR, please create a follow-up issue for adding the documentation
