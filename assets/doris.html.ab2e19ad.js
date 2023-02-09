import{_ as i}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as d,a as n,b as t,d as e,w as a,f as p,e as l,r}from"./app.de9af28d.js";const c={},u=n("h1",{id:"doris-连接器",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#doris-连接器","aria-hidden":"true"},"#"),t(" Doris 连接器")],-1),g=l(`<p><strong>BitSail</strong> Doris连接器支持批式和流式写doris，其支持的主要功能点如下</p><ul><li>使用StreamLoad方式写Doris表</li><li>支持分区创建再写入</li><li>支持多种table mode</li></ul><h2 id="依赖引入" tabindex="-1"><a class="header-anchor" href="#依赖引入" aria-hidden="true">#</a> 依赖引入</h2><div class="language-xml line-numbers-mode" data-ext="xml"><pre class="language-xml"><code><span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>dependency</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>groupId</span><span class="token punctuation">&gt;</span></span>com.bytedance.bitsail<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>groupId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>artifactId</span><span class="token punctuation">&gt;</span></span>bitsail-connector-doris<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>artifactId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>version</span><span class="token punctuation">&gt;</span></span>\${revision}<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>version</span><span class="token punctuation">&gt;</span></span>
<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>dependency</span><span class="token punctuation">&gt;</span></span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="doris写入" tabindex="-1"><a class="header-anchor" href="#doris写入" aria-hidden="true">#</a> Doris写入</h2><h3 id="支持的数据类型" tabindex="-1"><a class="header-anchor" href="#支持的数据类型" aria-hidden="true">#</a> 支持的数据类型</h3><p>写连接器使用 JSON 或者 CSV 格式传输数据，支持常见的 Doris 数据类型：</p><ul><li>CHAR</li><li>VARCHAR</li><li>TEXT</li><li>BOOLEAN</li><li>BINARY</li><li>VARBINARY</li><li>DECIMAL</li><li>DECIMALV2</li><li>INT</li><li>TINYINT</li><li>SMALLINT</li><li>INTEGER</li><li>INTERVAL_YEAR_MONTH</li><li>INTERVAL_DAY_TIME</li><li>BIGINT</li><li>LARGEINT</li><li>FLOAT</li><li>DOUBLE</li><li>DATE</li><li>DATETIME</li></ul><h3 id="主要参数" tabindex="-1"><a class="header-anchor" href="#主要参数" aria-hidden="true">#</a> 主要参数</h3><p>写连接器参数在<code>job.writer</code>中配置，实际使用时请注意路径前缀。示例:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.doris.sink.DorisSink&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_doris_table&quot;</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="必需参数" tabindex="-1"><a class="header-anchor" href="#必需参数" aria-hidden="true">#</a> 必需参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Doris写连接器类型, <code>com.bytedance.bitsail.connector.doris.sink.DorisSink</code></td></tr><tr><td style="text-align:left;">fe_hosts</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Doris FE地址, 多个地址用逗号分隔</td></tr><tr><td style="text-align:left;">mysql_hosts</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">JDBC连接Doris的地址, 多个地址用逗号分隔</td></tr><tr><td style="text-align:left;">user</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Doris账户</td></tr><tr><td style="text-align:left;">password</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Doris密码，可为空</td></tr><tr><td style="text-align:left;">db_name</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要写入的doris库</td></tr><tr><td style="text-align:left;">table_name</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要写入的doris表</td></tr><tr><td style="text-align:left;">partitions</td><td style="text-align:left;">分区表必需</td><td style="text-align:left;"></td><td style="text-align:left;">要写入的分区</td></tr><tr><td style="text-align:left;">table_has_partition</td><td style="text-align:left;">非分区表必需</td><td style="text-align:left;"></td><td style="text-align:left;">非分区表填写true</td></tr></tbody></table>`,13),f=l(`<p>注意，partitions格式要求如下:</p><ol><li>可写入多个partition，每个partition</li><li>每个partition内需要有: <ol><li><code>name</code>: 要写入的分区名</li><li><code>start_range</code>, <code>end_range</code>: 该分区范围</li></ol></li></ol><p>partitions示例:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;partitions&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
    <span class="token punctuation">{</span>
      <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;p20220210_03&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;start_range&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token string">&quot;2022-02-10&quot;</span><span class="token punctuation">,</span>
        <span class="token string">&quot;3&quot;</span>
      <span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;end_range&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token string">&quot;2022-02-10&quot;</span><span class="token punctuation">,</span>
        <span class="token string">&quot;4&quot;</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span><span class="token punctuation">,</span>
    <span class="token punctuation">{</span>
      <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;p20220211_03&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;start_range&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token string">&quot;2022-02-11&quot;</span><span class="token punctuation">,</span>
        <span class="token string">&quot;3&quot;</span>
      <span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;end_range&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token string">&quot;2022-02-11&quot;</span><span class="token punctuation">,</span>
        <span class="token string">&quot;4&quot;</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">]</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="可选参数" tabindex="-1"><a class="header-anchor" href="#可选参数" aria-hidden="true">#</a> 可选参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">writer_parallelism_num</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">指定Doris写并发</td></tr><tr><td style="text-align:left;">sink_flush_interval_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">Upsert模式下的flush间隔, 默认5000 ms</td></tr><tr><td style="text-align:left;">sink_max_retries</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">写入的最大重试次数，默认3</td></tr><tr><td style="text-align:left;">sink_buffer_size</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">写入buffer最大值，默认 20971520 bytes (20MB)</td></tr><tr><td style="text-align:left;">sink_buffer_count</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">写入buffer的最大条数，默认100000</td></tr><tr><td style="text-align:left;">sink_enable_delete</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否支持delete事件同步</td></tr><tr><td style="text-align:left;">sink_write_mode</td><td style="text-align:left;">否</td><td style="text-align:left;">目前只支持以下几种:<br>STREAMING_UPSERT<br>BATCH_UPSERT<br>BATCH_REPLACE</td><td style="text-align:left;">写入模式</td></tr><tr><td style="text-align:left;">stream_load_properties</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">追加在streamload url后的参数，map&lt;string,string&gt;格式</td></tr><tr><td style="text-align:left;">load_contend_type</td><td style="text-align:left;">否</td><td style="text-align:left;">csv<br>json</td><td style="text-align:left;">streamload使用的格式，默认json</td></tr><tr><td style="text-align:left;">csv_field_delimiter</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">csv格式的行内分隔符, 默认逗号 &quot;,&quot;</td></tr><tr><td style="text-align:left;">csv_line_delimiter</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">csv格式的行间分隔符, 默认 &quot;\\n&quot;</td></tr></tbody></table><p>sink_write_mode 参数可选值：</p><ul><li>STREAMING_UPSERT: 流式upsert写入</li><li>BATCH_UPSERT: 批式upsert写入</li><li>BATCH_REPLACE: 批式replace写入</li></ul><h2 id="相关文档" tabindex="-1"><a class="header-anchor" href="#相关文档" aria-hidden="true">#</a> 相关文档</h2>`,9);function k(y,x){const s=r("RouterLink");return o(),d("div",null,[u,n("p",null,[t("上级文档："),e(s,{to:"/zh/documents/connectors/"},{default:a(()=>[t("连接器")]),_:1})]),g,p("AGGREGATE<br/>DUPLICATE"),f,n("p",null,[t("配置示例文档："),e(s,{to:"/zh/documents/connectors/doris/doris-example.html"},{default:a(()=>[t("Doris 连接器示例")]),_:1})])])}const b=i(c,[["render",k],["__file","doris.html.vue"]]);export{b as default};
