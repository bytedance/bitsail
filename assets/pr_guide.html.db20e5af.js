import{_ as a,a as o,b as t,c as s,d,e as r,f as l,g as c,h as u,i as m}from"./create_pr.04d55cd8.js";import{_ as h}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as v,c as g,a as e,b as i,d as b,w as p,e as f,r as y}from"./app.3bb0cf85.js";const B={},_=e("h1",{id:"pull-request-guide",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#pull-request-guide","aria-hidden":"true"},"#"),i(" Pull Request Guide")],-1),w=f('<hr><p><img src="'+a+'" alt="" loading="lazy"></p><h2 id="fork-bitsail-to-your-repository" tabindex="-1"><a class="header-anchor" href="#fork-bitsail-to-your-repository" aria-hidden="true">#</a> Fork BitSail to your repository</h2><p><img src="'+o+`" alt="" loading="lazy"></p><h2 id="git-account-configuration" tabindex="-1"><a class="header-anchor" href="#git-account-configuration" aria-hidden="true">#</a> Git account configuration</h2><p>The role of user name and email address: User name and email address are variables of the local git client. Each commit will be recorded with the user name and email address. Github&#39;s contribution statistics are based on email addresses.</p><p>Check your account and email address:</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git config user.name
$ git config user.email
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>If you are using git for the first time, or need to modify account, execute the following command, replacing the username and email address with your own.</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git config --global user.name &quot;username&quot;
$ git config --global user.email &quot;your_email@example.com&quot;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="clone-the-fork-repository-to-local" tabindex="-1"><a class="header-anchor" href="#clone-the-fork-repository-to-local" aria-hidden="true">#</a> Clone the Fork repository to local</h2><p>You can choose HTTPS or SSH mode, and the following operations will use SSH mode as an example. If you use HTTPS mode, you only need to replace all the SSH url in the command with HTTPS url.</p><h3 id="https" tabindex="-1"><a class="header-anchor" href="#https" aria-hidden="true">#</a> HTTPS</h3><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git clone git@github.com:{your_github_id}/bitsail.git
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h3 id="ssh" tabindex="-1"><a class="header-anchor" href="#ssh" aria-hidden="true">#</a> SSH</h3><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git clone https://github.com/{your_github_id}/bitsail.git
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><p><img src="`+t+`" alt="" loading="lazy"></p><h2 id="set-origin-and-upstream" tabindex="-1"><a class="header-anchor" href="#set-origin-and-upstream" aria-hidden="true">#</a> Set origin and upstream</h2><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git remote add origin git@github.com:{your_github_id}/bitsail.git
$ git remote add upstream git@github.com:bytedance/bitsail.git
$ git remote -v
origin  git@github.com:{your_github_id}/bitsail.git (fetch)
origin  git@github.com:{your_github_id}/bitsail.git (push)
upstream        git@github.com:bytedance/bitsail.git (fetch)
upstream        git@github.com:bytedance/bitsail.git (push)
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>If the <code>origin</code> setting of <code>git</code> is wrong, you can execute <code>git </code><em><code>remote</code></em><code>rm</code><em><code>origin</code></em> to clear and reset it.</p><p>The <code>upstream</code> is the same, setting errors can be cleared by <code>git </code><em><code>remote</code></em><code>rm</code><em><code>upstream</code></em> and reset.</p><h2 id="create-your-working-branch" tabindex="-1"><a class="header-anchor" href="#create-your-working-branch" aria-hidden="true">#</a> Create your working branch</h2><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>// view all branches
$ git branch -a
// Create a new loacl branch 
$ git branch {your_branch_name}
// switch to new branch
$ git checkout {your_branch_name}
// Push the local branch to the fork repository
$ git push -u origin
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Branch name example: add-sink-connector-redis</p><p>After that, you can write and test the code in your own working branch, and synchronize it to your personal branch in time.</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git add .
$ git commit -m &quot;[BitSail] Message&quot;
$ git push -u origin &lt;branch name&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="synchronize-source-code" tabindex="-1"><a class="header-anchor" href="#synchronize-source-code" aria-hidden="true">#</a> Synchronize source code</h2><p>BitSail will carefully consider the update and iteration of the interface or version. If the developer has a short development cycle, he can do a synchronization with the original warehouse before submitting the code. However, if unfortunately encountering a major version change, the developer can follow up at any time Changes to the original repository.</p><p>Here, in order to ensure the cleanness of the code branch, it is recommended to use the rebase method for merging.</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git fetch upstream
$ git rebase upstream/master
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>During the rebase process, file conflicts may be reported</p><p>For example, in the following situation, we need to manually merge the conflicting files: <code>bitsail-connectors/pom.xml</code></p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git rebase upstream/master
Auto-merging bitsail-dist/pom.xml
Auto-merging bitsail-connectors/pom.xml
CONFLICT (content): Merge conflict in bitsail-connectors/pom.xml
error: could not apply 054a4d3... [BitSail] Migrate hadoop source&amp;sink to v1 interface
Resolve all conflicts manually, mark them as resolved with
&quot;git add/rm &lt;conflicted_files&gt;&quot;, then run &quot;git rebase --continue&quot;.
You can instead skip this commit: run &quot;git rebase --skip&quot;.
To abort and get back to the state before &quot;git rebase&quot;, run &quot;git rebase --abort&quot;.
Could not apply 054a4d3... [BitSail] Migrate hadoop source&amp;sink to v1 interface
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>The conflicting parts are shown below, bounded by <code>=======</code>, decide whether you want to keep only the changes of the branch, only the changes of the other branch, or make completely new changes (possibly containing changes of both branches). Remove the conflict markers <code>&lt;&lt;&lt;&lt;&lt;&lt;&lt;</code>, <code>=======</code>, <code>&gt;&gt;&gt;&gt;&gt;&gt;&gt;</code> and make the desired changes in the final merge.</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>&lt;modules&gt;
    &lt;module&gt;bitsail-connectors-legacy&lt;/module&gt;
    &lt;module&gt;connector-print&lt;/module&gt;
    &lt;module&gt;connector-elasticsearch&lt;/module&gt;
    &lt;module&gt;connector-fake&lt;/module&gt;
    &lt;module&gt;connector-base&lt;/module&gt;
    &lt;module&gt;connector-doris&lt;/module&gt;
    &lt;module&gt;connector-kudu&lt;/module&gt;
    &lt;module&gt;connector-rocketmq&lt;/module&gt;
    &lt;module&gt;connector-redis&lt;/module&gt;
    &lt;module&gt;connector-clickhouse&lt;/module&gt;
&lt;&lt;&lt;&lt;&lt;&lt;&lt; HEAD
    &lt;module&gt;connector-druid&lt;/module&gt;
=======
    &lt;module&gt;connector-hadoop&lt;/module&gt;
&gt;&gt;&gt;&gt;&gt;&gt;&gt; 054a4d3 ([BitSail] Migrate hadoop source&amp;sink to v1 interface)
&lt;/modules&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>After combine:</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>&lt;modules&gt;
    &lt;module&gt;bitsail-connectors-legacy&lt;/module&gt;
    &lt;module&gt;connector-print&lt;/module&gt;
    &lt;module&gt;connector-elasticsearch&lt;/module&gt;
    &lt;module&gt;connector-fake&lt;/module&gt;
    &lt;module&gt;connector-base&lt;/module&gt;
    &lt;module&gt;connector-doris&lt;/module&gt;
    &lt;module&gt;connector-kudu&lt;/module&gt;
    &lt;module&gt;connector-rocketmq&lt;/module&gt;
    &lt;module&gt;connector-redis&lt;/module&gt;
    &lt;module&gt;connector-clickhouse&lt;/module&gt;
    &lt;module&gt;connector-druid&lt;/module&gt;
    &lt;module&gt;connector-hadoop&lt;/module&gt;
&lt;/modules&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Execute <code>git add &lt;conflicted_files&gt;</code> after combine:</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git add bitsail-connectors/pom.xml
$ git rebase --continue
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>Afterwards, the following window will appear. This is the Vim editing interface. The editing mode can be done according to Vim. Usually we only need to edit the Commit information on the first line, or not. After completion, follow the exit method of Vim, and press<code>: w q ↵</code>。</p><p><img src="`+s+`" alt="" loading="lazy"></p><p>After that, the following appears to indicate that the rebase is successful.</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git rebase --continue
[detached HEAD 9dcf4ee] [BitSail] Migrate hadoop source&amp;sink to v1 interface
 15 files changed, 766 insertions(+)
 create mode 100644 bitsail-connectors/connector-hadoop/pom.xml
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/constant/HadoopConstants.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/error/TextInputFormatErrorCode.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/format/HadoopDeserializationSchema.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/option/HadoopReaderOptions.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/sink/HadoopSink.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/sink/HadoopWriter.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/source/HadoopSource.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/source/coordinator/HadoopSourceSplitCoordinator.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/source/reader/HadoopSourceReader.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/source/reader/HadoopSourceReaderCommonBasePlugin.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/java/com/bytedance/bitsail/connector/hadoop/source/split/HadoopSourceSplit.java
 create mode 100644 bitsail-connectors/connector-hadoop/src/main/resources/bitsail-connector-unified-hadoop.json
Successfully rebased and updated refs/heads/add-v1-connector-hadoop.
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>At this point, we can see that our <code>commit</code> has been mentioned on the front:</p><p><img src="`+d+`" alt="" loading="lazy"></p><p>The code may not be pushed normally after rebase:</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git push
To github.com:love-star/bitsail.git
 ! [rejected]        add-v1-connector-hadoop -&gt; add-v1-connector-hadoop (non-fast-forward)
error: failed to push some refs to &#39;github.com:love-star/bitsail.git&#39;
hint: Updates were rejected because the tip of your current branch is behind
hint: its remote counterpart. Integrate the remote changes (e.g.
hint: &#39;git pull ...&#39;) before pushing again.
hint: See the &#39;Note about fast-forwards&#39; in &#39;git push --help&#39; for details.
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>At this time, <code>git push -f</code> is required to force the push. Forced push is a risky operation. Please check carefully before the operation to avoid the problem that irrelevant code is forcibly overwritten.</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>git push -f
Enumerating objects: 177, done.
Counting objects: 100% (177/177), done.
Delta compression using up to 12 threads
Compressing objects: 100% (110/110), done.
Writing objects: 100% (151/151), 26.55 KiB | 1.40 MiB/s, done.
Total 151 (delta 40), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (40/40), completed with 10 local objects.
To github.com:love-star/bitsail.git
 + adb90f4...b72d931 add-v1-connector-hadoop -&gt; add-v1-connector-hadoop (forced update)
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>At this point, the branch has been synchronized with the upstream repository, and subsequent code writing will be based on the latest.</p><h2 id="submit-your-code" tabindex="-1"><a class="header-anchor" href="#submit-your-code" aria-hidden="true">#</a> Submit your code</h2><p>When the developer completes the development, he first needs to complete a <code>rebase</code> of the warehouse. For details, refer to the scenario of <code>synchronizing source code</code>. After rebase, git&#39;s history looks like this:</p><p><img src="`+r+'" alt="" loading="lazy"></p><p>As shown on Github</p><p><img src="'+l+`" alt="" loading="lazy"></p><p>We hope to keep only one Commit for each PR to ensure the cleanness of the branch. If there are multiple commits, they can be merged into one commit in the end. The specific operation is as follows:</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>git reset --soft HEAD~N(N is the reset submit number)
git add .
git commit -m &quot;[BitSail] Message&quot;
git push -f
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>example:</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git reset --soft HEAD~4
$ git add .
$ git commit -m &quot;[BitSail] Migrate hadoop source&amp;sink to v1 interface&quot;
$ git push -f
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>After the reset:</p><p><img src="`+c+'" alt="" loading="lazy"></p><h2 id="submit-your-pr" tabindex="-1"><a class="header-anchor" href="#submit-your-pr" aria-hidden="true">#</a> Submit your PR</h2><p><img src="'+u+'" alt="" loading="lazy"></p><p>When submitting PR, you should pay attention to the specifications of Commit message and PR message:</p><p><img src="'+m+`" alt="" loading="lazy"></p><h3 id="commit-message-specification" tabindex="-1"><a class="header-anchor" href="#commit-message-specification" aria-hidden="true">#</a> Commit message specification</h3><ol><li>Create a Github issue or claim an existing issue</li><li>Describe what you would like to do in the issue description.</li><li>Include the issue number in the commit message. The format follows below.</li></ol><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>[BitSail#\${IssueNumber}][\${Module}] Description
[BitSail#1234][Connector] Improve reader split algorithm to Kudu source connector

//For Minor change
[Minor] Description
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ol><li>List of module. Chose the most related one if your changes affect multiple modules. e.g. If you are adding a feature to the kafka connector and end up modifying code in common, components and cores, you should still use the [Connector] as module name.</li></ol><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>[Common] bitsail-common
[Core] base client component cores
[Connector] all connector related changes
[Doc] documentation or java doc changes
[Build] build, dependency changes
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="pr-message-specification" tabindex="-1"><a class="header-anchor" href="#pr-message-specification" aria-hidden="true">#</a> PR message specification</h3><p>The PR message should summarize the cause and effect of the problem clearly. If there is a corresponding issue, the issue address should be attached to ensure that the problem is traceable.</p>`,72);function x(k,S){const n=y("RouterLink");return v(),g("div",null,[_,e("p",null,[i("English | "),b(n,{to:"/zh/community/pr_guide.html"},{default:p(()=>[i("简体中文")]),_:1})]),w])}const C=h(B,[["render",x],["__file","pr_guide.html.vue"]]);export{C as default};
