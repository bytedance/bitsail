import{_ as p}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as r,c as d,a as t,b as e,d as a,w as s,e as o,r as l}from"./app.de9af28d.js";const u={},c=t("h1",{id:"hudi-connector",tabindex:"-1"},[t("a",{class:"header-anchor",href:"#hudi-connector","aria-hidden":"true"},"#"),e(" Hudi connector")],-1),h=o(`<p>The <strong>BitSail</strong> hudi connector supports reading and writing to hudi tables. The main function points are as follows:</p><ul><li>Support streaming write to Hudi table.</li><li>Support batch write to Hudi table.</li><li>Support batch read from Hudi table.</li></ul><h2 id="supported-hudi-versions" tabindex="-1"><a class="header-anchor" href="#supported-hudi-versions" aria-hidden="true">#</a> Supported hudi versions</h2><ul><li>0.11.1</li></ul><h2 id="maven-dependency" tabindex="-1"><a class="header-anchor" href="#maven-dependency" aria-hidden="true">#</a> Maven dependency</h2><div class="language-xml line-numbers-mode" data-ext="xml"><pre class="language-xml"><code><span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>dependency</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>groupId</span><span class="token punctuation">&gt;</span></span>com.bytedance.bitsail<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>groupId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>artifactId</span><span class="token punctuation">&gt;</span></span>bitsail-connector-hudi<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>artifactId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>version</span><span class="token punctuation">&gt;</span></span>\${revision}<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>version</span><span class="token punctuation">&gt;</span></span>
<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>dependency</span><span class="token punctuation">&gt;</span></span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="hudi-reader" tabindex="-1"><a class="header-anchor" href="#hudi-reader" aria-hidden="true">#</a> Hudi reader</h2><h3 id="supported-data-types" tabindex="-1"><a class="header-anchor" href="#supported-data-types" aria-hidden="true">#</a> Supported data types</h3><ul><li></li><li>Basic Data types: <ul><li>Integer type: <ul><li>tinyint</li><li>smallint</li><li>int</li><li>bigint</li></ul></li><li>Float type: <ul><li>float</li><li>double</li><li>decimal</li></ul></li><li>Time type: <ul><li>timestamp</li><li>date</li></ul></li><li>String type: <ul><li>string</li><li>varchar</li><li>char</li></ul></li><li>Bool type: <ul><li>boolean</li></ul></li><li>Binary type: <ul><li>binary</li></ul></li></ul></li><li>Composited data types: <ul><li>map</li><li>array</li></ul></li></ul><h3 id="parameters" tabindex="-1"><a class="header-anchor" href="#parameters" aria-hidden="true">#</a> Parameters</h3><p>The following mentioned parameters should be added to <code>job.reader</code> block when using, for example:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;hoodie&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;datasource&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
          <span class="token property">&quot;query&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;snapshot&quot;</span>
          <span class="token punctuation">}</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span><span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;table&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;MERGE_ON_READ&quot;</span>
      <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="necessary-parameters" tabindex="-1"><a class="header-anchor" href="#necessary-parameters" aria-hidden="true">#</a> Necessary parameters</h4><table><thead><tr><th style="text-align:left;">Param name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Optional value</th><th style="text-align:left;">Description</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">Hudi read connector class name, <code>com.bytedance.bitsail.connector.legacy.hudi.dag.HudiSourceFunctionDAGBuilder</code></td></tr><tr><td style="text-align:left;">path</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">the path of the table, could be HDFS, S3, or other file systems.</td></tr><tr><td style="text-align:left;">table.type</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">The type of the Hudi table, MERGE_ON_READ or COPY_ON_WRITE</td></tr><tr><td style="text-align:left;">hoodie.datasource.query.type</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">Query type, could be <code>snapshot</code> or <code>read_optimized</code></td></tr></tbody></table><h4 id="optional-parameters" tabindex="-1"><a class="header-anchor" href="#optional-parameters" aria-hidden="true">#</a> Optional parameters</h4><table><thead><tr><th style="text-align:left;">Param name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Optional value</th><th style="text-align:left;">Description</th></tr></thead><tbody><tr><td style="text-align:left;">reader_parallelism_num</td><td style="text-align:left;">No</td><td style="text-align:left;"></td><td style="text-align:left;">Read parallelism num</td></tr></tbody></table><h2 id="hudi-writer" tabindex="-1"><a class="header-anchor" href="#hudi-writer" aria-hidden="true">#</a> Hudi writer</h2><h3 id="supported-data-type" tabindex="-1"><a class="header-anchor" href="#supported-data-type" aria-hidden="true">#</a> Supported data type</h3><ul><li>Basic data types supported: <ul><li>Integer type: <ul><li>tinyint</li><li>smallint</li><li>int</li><li>bigint</li></ul></li><li>Float type: <ul><li>float</li><li>double</li><li>decimal</li></ul></li><li>Time type: <ul><li>timestamp</li><li>date</li></ul></li><li>String type: <ul><li>string</li><li>varchar</li><li>char</li></ul></li><li>Bool type: <ul><li>boolean</li></ul></li><li>Binary type: <ul><li>binary</li></ul></li></ul></li><li>Composited data types supported: <ul><li>map</li><li>array</li></ul></li></ul><h3 id="parameters-1" tabindex="-1"><a class="header-anchor" href="#parameters-1" aria-hidden="true">#</a> Parameters</h3><p>The following mentioned parameters should be added to <code>job.writer</code> block when using, for example:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;hoodie&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;bucket&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
            <span class="token property">&quot;num&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
              <span class="token property">&quot;buckets&quot;</span><span class="token operator">:</span> <span class="token string">&quot;4&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token property">&quot;hash&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
              <span class="token property">&quot;field&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span>
            <span class="token punctuation">}</span>
          <span class="token punctuation">}</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token property">&quot;datasource&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;write&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
            <span class="token property">&quot;recordkey&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
              <span class="token property">&quot;field&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span>
            <span class="token punctuation">}</span>
          <span class="token punctuation">}</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token property">&quot;table&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_table&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span> <span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;BUCKET&quot;</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;write&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;operation&quot;</span><span class="token operator">:</span> <span class="token string">&quot;upsert&quot;</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;table&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;MERGE_ON_READ&quot;</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;source_schema&quot;</span><span class="token operator">:</span> <span class="token string">&quot;[{\\&quot;name\\&quot;:\\&quot;id\\&quot;,\\&quot;type\\&quot;:\\&quot;bigint\\&quot;},{\\&quot;name\\&quot;:\\&quot;test\\&quot;,\\&quot;type\\&quot;:\\&quot;string\\&quot;},{\\&quot;name\\&quot;:\\&quot;timestamp\\&quot;,\\&quot;type\\&quot;:\\&quot;string\\&quot;}]&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;sink_schema&quot;</span><span class="token operator">:</span> <span class="token string">&quot;[{\\&quot;name\\&quot;:\\&quot;id\\&quot;,\\&quot;type\\&quot;:\\&quot;bigint\\&quot;},{\\&quot;name\\&quot;:\\&quot;test\\&quot;,\\&quot;type\\&quot;:\\&quot;string\\&quot;},{\\&quot;name\\&quot;:\\&quot;timestamp\\&quot;,\\&quot;type\\&quot;:\\&quot;string\\&quot;}]&quot;</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="necessary-parameters-1" tabindex="-1"><a class="header-anchor" href="#necessary-parameters-1" aria-hidden="true">#</a> Necessary parameters</h4>`,23),y=t("thead",null,[t("tr",null,[t("th",{style:{"text-align":"left"}},"Param name"),t("th",{style:{"text-align":"left"}},"Is necessary"),t("th",{style:{"text-align":"left"}},"Optional value"),t("th",{style:{"text-align":"left"}},"Description")])],-1),m=t("tr",null,[t("td",{style:{"text-align":"left"}},"class"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},[e("Hudi write class name, "),t("code",null,"com.bytedance.bitsail.connector.legacy.hudi.sink.HudiSinkFunctionDAGBuilder")])],-1),k=t("tr",null,[t("td",{style:{"text-align":"left"}},"write.operation"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},[t("code",null,"upsert"),e(),t("code",null,"insert"),e(),t("code",null,"bulk_insert")])],-1),g=t("tr",null,[t("td",{style:{"text-align":"left"}},"table.type"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},[t("code",null,"MERGE_ON_READ"),e(),t("code",null,"COPY_ON_WRITE")])],-1),b=t("tr",null,[t("td",{style:{"text-align":"left"}},"path"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},"path to the Hudi table, could be HDFS, S3, or other file system. If path not exists, the table will be created on this path.")],-1),v=t("tr",null,[t("td",{style:{"text-align":"left"}},"format_type"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},[e("format of the input data source, currently only support "),t("code",null,"json")])],-1),f=t("tr",null,[t("td",{style:{"text-align":"left"}},"source_schema"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},"schema used to deserialize source data.")],-1),q=t("tr",null,[t("td",{style:{"text-align":"left"}},"sink_schema"),t("td",{style:{"text-align":"left"}},"Yes"),t("td",{style:{"text-align":"left"}}),t("td",{style:{"text-align":"left"}},"schema used to write hoodie data")],-1),x={style:{"text-align":"left"}},_={href:"http://hoodie.table.name",target:"_blank",rel:"noopener noreferrer"},w=t("td",{style:{"text-align":"left"}},"Yes",-1),S=t("td",{style:{"text-align":"left"}},null,-1),H=t("td",{style:{"text-align":"left"}},"the name of the hoodie table",-1),D=o(`<h4 id="optional-parameters-1" tabindex="-1"><a class="header-anchor" href="#optional-parameters-1" aria-hidden="true">#</a> Optional parameters</h4><p>For more advance parameter, please checkout <code>FlinkOptions.java</code> class.</p><table><thead><tr><th style="text-align:left;">Param name</th><th style="text-align:left;">Is necessary</th><th style="text-align:left;">Optional value</th><th style="text-align:left;">Description</th></tr></thead><tbody><tr><td style="text-align:left;">hoodie.datasource.write.recordkey.field</td><td style="text-align:left;">false</td><td style="text-align:left;"></td><td style="text-align:left;">For <code>upsert</code> operation, we need to define the primary key.</td></tr><tr><td style="text-align:left;">index.type</td><td style="text-align:left;">false</td><td style="text-align:left;"></td><td style="text-align:left;">For <code>upsert</code> operation, we need to define the index type. could be <code>STATE</code> or <code>BUCKET</code></td></tr><tr><td style="text-align:left;">hoodie.bucket.index.num.buckets</td><td style="text-align:left;">false</td><td style="text-align:left;"></td><td style="text-align:left;">If we use Bucket index, we need to define the bucket number.</td></tr><tr><td style="text-align:left;">hoodie.bucket.index.hash.field</td><td style="text-align:left;">false</td><td style="text-align:left;"></td><td style="text-align:left;">If we use Bucket index, we need to define a field to determine hash index.</td></tr></tbody></table><h2 id="hudi-compaction" tabindex="-1"><a class="header-anchor" href="#hudi-compaction" aria-hidden="true">#</a> Hudi Compaction</h2><h3 id="parameters-2" tabindex="-1"><a class="header-anchor" href="#parameters-2" aria-hidden="true">#</a> Parameters</h3><p>Compaction has well-defined reader and writer parameters</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span><span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder&quot;</span>
    <span class="token punctuation">}</span><span class="token punctuation">,</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span><span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder&quot;</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="necessary-parameters-2" tabindex="-1"><a class="header-anchor" href="#necessary-parameters-2" aria-hidden="true">#</a> Necessary parameters</h4><table><thead><tr><th style="text-align:left;">Param name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Optional value</th><th style="text-align:left;">Description</th></tr></thead><tbody><tr><td style="text-align:left;">job.reader.class</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">Hudi compaction read connector class name, <code>com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSourceDAGBuilder</code></td></tr><tr><td style="text-align:left;">job.writer.class</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">Hudi compaction writer connector class name, <code>com.bytedance.bitsail.connector.legacy.hudi.dag.HudiCompactSinkDAGBuilder</code></td></tr><tr><td style="text-align:left;">job.reader.path</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">the path of the table, could be HDFS, S3, or other file systems.</td></tr><tr><td style="text-align:left;">job.writer.path</td><td style="text-align:left;">Yes</td><td style="text-align:left;"></td><td style="text-align:left;">the path of the table, could be HDFS, S3, or other file systems.</td></tr></tbody></table><h4 id="optional-parameters-2" tabindex="-1"><a class="header-anchor" href="#optional-parameters-2" aria-hidden="true">#</a> Optional parameters</h4><table><thead><tr><th style="text-align:left;">Param name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Optional value</th><th style="text-align:left;">Description</th></tr></thead><tbody><tr><td style="text-align:left;">writer_parallelism_num</td><td style="text-align:left;">No</td><td style="text-align:left;"></td><td style="text-align:left;">parallelism to process the compaction</td></tr></tbody></table><h2 id="related-documents" tabindex="-1"><a class="header-anchor" href="#related-documents" aria-hidden="true">#</a> Related documents</h2>`,12);function B(j,E){const n=l("RouterLink"),i=l("ExternalLinkIcon");return r(),d("div",null,[c,t("p",null,[e("Parent document: "),a(n,{to:"/en/documents/connectors/"},{default:s(()=>[e("Connectors")]),_:1})]),h,t("table",null,[y,t("tbody",null,[m,k,g,b,v,f,q,t("tr",null,[t("td",x,[t("a",_,[e("hoodie.table.name"),a(i)])]),w,S,H])])]),D,t("p",null,[e("Configuration examples: "),a(n,{to:"/en/documents/connectors/hudi/hudi-example.html"},{default:s(()=>[e("Hudi connector example")]),_:1})])])}const Y=p(u,[["render",B],["__file","hudi.html.vue"]]);export{Y as default};
