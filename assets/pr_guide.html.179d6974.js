import{_ as n,a as s,b as d,c as o,d as t,e as l,f as r,g as c,h as m,i as u}from"./create_pr.04d55cd8.js";import{_ as v}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as g,c as b,a as e,d as h,w as p,b as i,e as f,r as B}from"./app.982a74e2.js";const _={},x=e("h1",{id:"pr发布指南",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#pr发布指南","aria-hidden":"true"},"#"),i(" PR发布指南")],-1),y=f('<hr><p><img src="'+n+'" alt="" loading="lazy"></p><h2 id="fork-bitsail-到自己的仓库" tabindex="-1"><a class="header-anchor" href="#fork-bitsail-到自己的仓库" aria-hidden="true">#</a> Fork BitSail 到自己的仓库</h2><p><img src="'+s+`" alt="" loading="lazy"></p><h2 id="git的账户配置" tabindex="-1"><a class="header-anchor" href="#git的账户配置" aria-hidden="true">#</a> Git的账户配置</h2><p>用户名和邮箱地址的作用：用户名和邮箱地址是本地git客户端的一个变量，每次commit都会用用户名和邮箱纪录，Github的contributions统计就是按邮箱来统计的。</p><p>查看自己的账户和邮箱地址：</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git config user.name
$ git config user.email
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>如果是第一次使用，或者需要对其进行修改，执行以下命令，将用户名和邮箱地址替换为你自己的即可。</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git config --global user.name &quot;username&quot;
$ git config --global user.email &quot;your_email@example.com&quot;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="将fork仓库克隆到本地" tabindex="-1"><a class="header-anchor" href="#将fork仓库克隆到本地" aria-hidden="true">#</a> 将Fork仓库克隆到本地</h2><p>可选HTTPS或者SSH方式，之后的操作会以SSH方式示例，如果采用HTTPS方式，只需要将命令中的SSH地址全部替换为HTTPS地址即可。</p><h3 id="https" tabindex="-1"><a class="header-anchor" href="#https" aria-hidden="true">#</a> HTTPS</h3><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git clone git@github.com:{your_github_id}/bitsail.git
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h3 id="ssh" tabindex="-1"><a class="header-anchor" href="#ssh" aria-hidden="true">#</a> SSH</h3><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git clone https://github.com/{your_github_id}/bitsail.git
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><p><img src="`+d+`" alt="" loading="lazy"></p><h2 id="设置origin和upstream" tabindex="-1"><a class="header-anchor" href="#设置origin和upstream" aria-hidden="true">#</a> 设置origin和upstream</h2><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git remote add origin git@github.com:{your_github_id}/bitsail.git
$ git remote add upstream git@github.com:bytedance/bitsail.git
$ git remote -v
origin  git@github.com:{your_github_id}/bitsail.git (fetch)
origin  git@github.com:{your_github_id}/bitsail.git (push)
upstream        git@github.com:bytedance/bitsail.git (fetch)
upstream        git@github.com:bytedance/bitsail.git (push)
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>如果<code>git</code>的<code>origin</code>设置错误，可以执行<code>git </code><em><code>remote</code></em><code>rm</code>*<code>origin</code>*<em>清除后重新设置</em></p><p><code>upstream</code>同理，设置错误可以通过<code>git </code><em><code>remote</code></em><code>rm</code>*<code>upstream</code>*清除后重新设置</p><h2 id="创建自己的工作分支" tabindex="-1"><a class="header-anchor" href="#创建自己的工作分支" aria-hidden="true">#</a> 创建自己的工作分支</h2><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>查看所有分支
$ git branch -a
在本地新建一个分支
$ git branch {your_branch_name}
切换到我的新分支
$ git checkout {your_branch_name}
将本地分支推送到fork仓库
$ git push -u origin {your_branch_name}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>分支名称示例：add-sink-connector-redis</p><p>之后就可以在自己的工作分支进行代码的编写，测试，并及时同步到你的个人分支。</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>编辑区添加到暂存区
$ git add .
暂存区提交到分支
$ git commit -m &quot;[BitSail] Message&quot;
同步Fork仓库
$ git push -u origin &lt;分支名&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="同步代码" tabindex="-1"><a class="header-anchor" href="#同步代码" aria-hidden="true">#</a> 同步代码</h2><p>BitSail对接口或者版本的更新迭代会谨慎的考量，如果开发者开发周期短，可以在提交代码前对原始仓库做一次同步即可，但是如果不幸遇到了大的版本变更，开发者可以随时跟进对原始仓库的变更。</p><p>这里为了保证代码分支的干净，推荐采用rebase的方式进行合并。</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git fetch upstream
$ git rebase upstream/master
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>在rebase过程中，有可能会报告文件的冲突</p><p>例如如下情况，我们要去手动合并产生冲突的文件<code>bitsail-connectors/pom.xml</code></p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git rebase upstream/master
Auto-merging bitsail-dist/pom.xml
Auto-merging bitsail-connectors/pom.xml
CONFLICT (content): Merge conflict in bitsail-connectors/pom.xml
error: could not apply 054a4d3... [BitSail] Migrate hadoop source&amp;sink to v1 interface
Resolve all conflicts manually, mark them as resolved with
&quot;git add/rm &lt;conflicted_files&gt;&quot;, then run &quot;git rebase --continue&quot;.
You can instead skip this commit: run &quot;git rebase --skip&quot;.
To abort and get back to the state before &quot;git rebase&quot;, run &quot;git rebase --abort&quot;.
Could not apply 054a4d3... [BitSail] Migrate hadoop source&amp;sink to v1 interface
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>产生冲突的部分如下所示，<code>=======</code>为界， 决定您是否想只保持分支的更改、只保持其他分支的更改，还是进行全新的更改（可能包含两个分支的更改）。 删除冲突标记<code> &lt;&lt;&lt;&lt;&lt;&lt;&lt;</code>、<code>=======</code>、<code>&gt;&gt;&gt;&gt;&gt;&gt;&gt;</code>，并在最终合并中进行所需的更改。</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>&lt;modules&gt;
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>处理完成的示例：</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>&lt;modules&gt;
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>处理完成之后执行<code>git add &lt;conflicted_files&gt;</code>，比如该例中执行：</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git add bitsail-connectors/pom.xml
$ git rebase --continue
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><p>之后会出现如下窗口，这个是Vim编辑界面，编辑模式按照Vim的进行即可，通常我们只需要对第一行进行Commit信息进行编辑，也可以不修改，完成后按照Vim的退出方式，依次按<code>: w q 回车</code>即可。</p><p><img src="`+o+`" alt="" loading="lazy"></p><p>之后出现如下表示rebase成功。</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git rebase --continue
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>此时可以看到我们的<code>commit</code>已经被提到了最前面：</p><p><img src="`+t+`" alt="" loading="lazy"></p><p>rebase之后代码可能无法正常推送</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git push
To github.com:love-star/bitsail.git
 ! [rejected]        add-v1-connector-hadoop -&gt; add-v1-connector-hadoop (non-fast-forward)
error: failed to push some refs to &#39;github.com:love-star/bitsail.git&#39;
hint: Updates were rejected because the tip of your current branch is behind
hint: its remote counterpart. Integrate the remote changes (e.g.
hint: &#39;git pull ...&#39;) before pushing again.
hint: See the &#39;Note about fast-forwards&#39; in &#39;git push --help&#39; for details.
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>此时需要<code>git push -f</code> 强制推送，强制推送是一个有风险的操作，操作前请仔细检查以避免出现无关代码被强制覆盖的问题。</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git push -f
Enumerating objects: 177, done.
Counting objects: 100% (177/177), done.
Delta compression using up to 12 threads
Compressing objects: 100% (110/110), done.
Writing objects: 100% (151/151), 26.55 KiB | 1.40 MiB/s, done.
Total 151 (delta 40), reused 0 (delta 0), pack-reused 0
remote: Resolving deltas: 100% (40/40), completed with 10 local objects.
To github.com:love-star/bitsail.git
 + adb90f4...b72d931 add-v1-connector-hadoop -&gt; add-v1-connector-hadoop (forced update)
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>此时分支已经和原始仓库同步，之后的代码编写都会建立在最新的基础上。</p><h2 id="提交代码" tabindex="-1"><a class="header-anchor" href="#提交代码" aria-hidden="true">#</a> 提交代码</h2><p>当开发者开发完毕，首先需要完成一次仓库的rebase，具体参考同步代码的场景。rebase之后，git的历史如下所示：</p><p><img src="`+l+'" alt="" loading="lazy"></p><p>在Github界面如图所示</p><p><img src="'+r+`" alt="" loading="lazy"></p><p>我们希望在提交PR前仅仅保留一个Commit以保证分支的干净，如果有多次提交，最后可以合并为一个提交。具体操作如下：</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git reset --soft HEAD~N(N为需要合并的提交次数)
$ git add .
$ git commit -m &quot;[BitSail] Message&quot;
$ git push -f
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>比如此例中，执行</p><div class="language-Bash line-numbers-mode" data-ext="Bash"><pre class="language-Bash"><code>$ git reset --soft HEAD~4
$ git add .
$ git commit -m &quot;[BitSail#106][Connector] Migrate hadoop source connector to v1 interface&quot;
$ git push -f
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>合并后：</p><p><img src="`+c+'" alt="" loading="lazy"></p><h2 id="提交pr" tabindex="-1"><a class="header-anchor" href="#提交pr" aria-hidden="true">#</a> 提交PR</h2><p><img src="'+m+'" alt="" loading="lazy"></p><p>提交PR时，应注意Commit message和PR message的规范：</p><p><img src="'+u+`" alt="" loading="lazy"></p><h3 id="commit-message-规范" tabindex="-1"><a class="header-anchor" href="#commit-message-规范" aria-hidden="true">#</a> Commit message 规范</h3><ol><li>创建一个新的Github issue或者关联一个已经存在的 issue</li><li>在issue description中描述你想要进行的工作.</li><li>在commit message关联你的issue，格式如下：</li></ol><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>[BitSail#\${IssueNumber}][\${Module}] Description
[BitSail#1234][Connector] Improve reader split algorithm to Kudu source connector

//For Minor change
[Minor] Description
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ol><li>commit message的module格式列表如下，如果开发者的工作关联了多个module，选择最相关的module即可，例如：如果你在 kafka connector添加了新的feature，并且改变了common、components和cores中的代码，这时commit message应该绑定的module格式为[Connector]。</li></ol><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>[Common] bitsail-common
[Core] base client component cores
[Connector] all connector related changes
[Doc] documentation or java doc changes
[Build] build, dependency changes
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>注意</p><ul><li>commit 需遵循规范，给维护者减少维护成本及工作量，对于不符合规范的commit，我们不予合并。</li><li>对于解决同一个Issue的PR，只能存在一个commit message，如果出现多次提交的message，我们希望你能将commit message 压缩成一个。</li><li>message 尽量保持清晰简洁，但是也千万不要因为过度追求简洁导致描述不清楚，如果有必要，我们也不介意message过长，前提是，能够把解决方案、修复内容描述清楚。</li></ul><h3 id="pr-message规范" tabindex="-1"><a class="header-anchor" href="#pr-message规范" aria-hidden="true">#</a> PR message规范</h3><p>PR message应概括清楚问题的前因后果，如果存在对应issue要附加issue地址，保证问题是可追溯的。</p>`,74);function k(S,$){const a=B("RouterLink");return g(),b("div",null,[x,e("p",null,[h(a,{to:"/en/community/pr_guide.html"},{default:p(()=>[i("English")]),_:1}),i(" | 简体中文")]),y])}const H=v(_,[["render",k],["__file","pr_guide.html.vue"]]);export{H as default};
