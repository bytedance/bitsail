import{_ as s}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as d,c as i,a as e,b as t,d as a,w as n,e as o,r as u}from"./app.de9af28d.js";const r={},c=e("h1",{id:"kudu-连接器",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#kudu-连接器","aria-hidden":"true"},"#"),t(" Kudu 连接器")],-1),p=o(`<p><strong>BitSail</strong> Kudu 连接器支持批式读写 Kudu 表。</p><h2 id="依赖引入" tabindex="-1"><a class="header-anchor" href="#依赖引入" aria-hidden="true">#</a> 依赖引入</h2><div class="language-xml line-numbers-mode" data-ext="xml"><pre class="language-xml"><code><span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>dependency</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>groupId</span><span class="token punctuation">&gt;</span></span>com.bytedance.bitsail<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>groupId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>artifactId</span><span class="token punctuation">&gt;</span></span>connector-kudu<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>artifactId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>version</span><span class="token punctuation">&gt;</span></span>\${revision}<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>version</span><span class="token punctuation">&gt;</span></span>
<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>dependency</span><span class="token punctuation">&gt;</span></span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="kudu读取" tabindex="-1"><a class="header-anchor" href="#kudu读取" aria-hidden="true">#</a> Kudu读取</h2><p>Kudu通过scanner扫描数据表，支持常见的Kudu数据类型:</p><ul><li>整型: <code>int8, int16, int32, int64</code>&#39;</li><li>浮点类型: <code>float, double, decimal</code></li><li>布尔类型: <code>boolean</code></li><li>日期类型: <code>date, timestamp</code></li><li>字符类型: <code>string, varchar</code></li><li>二进制类型: <code>binary, string_utf8</code></li></ul><h3 id="主要参数" tabindex="-1"><a class="header-anchor" href="#主要参数" aria-hidden="true">#</a> 主要参数</h3><p>读连接器参数在<code>job.reader</code>中配置，实际使用时请注意路径前缀。示例:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.kudu.source.KuduSource&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kudu_table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;kudu_test_table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kudu_master_address_list&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span><span class="token string">&quot;localhost:1234&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;localhost:4321&quot;</span><span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="必需参数" tabindex="-1"><a class="header-anchor" href="#必需参数" aria-hidden="true">#</a> 必需参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu读连接器类型, <code>com.bytedance.bitsail.connector.kudu.source.KuduSource</code></td></tr><tr><td style="text-align:left;">kudu_table_name</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要读取的Kudu表</td></tr><tr><td style="text-align:left;">kudu_master_address_list</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu master地址, List形式表示</td></tr><tr><td style="text-align:left;">columns</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要读取的数据列的列名和类型</td></tr><tr><td style="text-align:left;">reader_parallelism_num</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">读并发</td></tr></tbody></table><h4 id="kuduclient相关参数" tabindex="-1"><a class="header-anchor" href="#kuduclient相关参数" aria-hidden="true">#</a> KuduClient相关参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">kudu_admin_operation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu client进行admin操作的timeout, 单位ms, 默认30000ms</td></tr><tr><td style="text-align:left;">kudu_operation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu client普通操作的timeout, 单位ms, 默认30000ms</td></tr><tr><td style="text-align:left;">kudu_connection_negotiation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">单位ms，默认10000ms</td></tr><tr><td style="text-align:left;">kudu_disable_client_statistics</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否启用client段statistics统计</td></tr><tr><td style="text-align:left;">kudu_worker_count</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">client内worker数量</td></tr><tr><td style="text-align:left;">sasl_protocol_name</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">默认 &quot;kudu&quot;</td></tr><tr><td style="text-align:left;">require_authentication</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否开启鉴权</td></tr><tr><td style="text-align:left;">encryption_policy</td><td style="text-align:left;">否</td><td style="text-align:left;">OPTIONAL<br>REQUIRED_REMOTE<br>REQUIRED</td><td style="text-align:left;">加密策略</td></tr></tbody></table><h4 id="kuduscanner相关参数" tabindex="-1"><a class="header-anchor" href="#kuduscanner相关参数" aria-hidden="true">#</a> KuduScanner相关参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">read_mode</td><td style="text-align:left;">否</td><td style="text-align:left;">READ_LATEST<br>READ_AT_SNAPSHOT</td><td style="text-align:left;">读取模式</td></tr><tr><td style="text-align:left;">snapshot_timestamp_us</td><td style="text-align:left;">read_mode=READ_AT_SNAPSHOT时必需</td><td style="text-align:left;"></td><td style="text-align:left;">指定要读取哪个时间点的snapshot</td></tr><tr><td style="text-align:left;">enable_fault_tolerant</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否允许fault tolerant</td></tr><tr><td style="text-align:left;">scan_batch_size_bytes</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">单batch内拉取的最大数据量</td></tr><tr><td style="text-align:left;">scan_max_count</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">最多拉取多少条数据</td></tr><tr><td style="text-align:left;">enable_cache_blocks</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否启用cache blocks, 默认true</td></tr><tr><td style="text-align:left;">scan_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">scan超时时间, 单位ms, 默认30000ms</td></tr><tr><td style="text-align:left;">scan_keep_alive_period_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;"></td></tr></tbody></table><h4 id="分片相关参数" tabindex="-1"><a class="header-anchor" href="#分片相关参数" aria-hidden="true">#</a> 分片相关参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">split_strategy</td><td style="text-align:left;">否</td><td style="text-align:left;">SIMPLE_DIVIDE</td><td style="text-align:left;">分片策略, 目前只支持 SIMPLE_DIVIDE</td></tr><tr><td style="text-align:left;">split_config</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">各个分片策略对应的配置</td></tr></tbody></table><h5 id="simple-divide分片策略" tabindex="-1"><a class="header-anchor" href="#simple-divide分片策略" aria-hidden="true">#</a> SIMPLE_DIVIDE分片策略</h5><p>SIMPLE_DIVIDE对应的split_config格式如下:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>&quot;{\\&quot;name\\&quot;: \\&quot;key\\&quot;, \\&quot;lower_bound\\&quot;: 0, \\&quot;upper_bound\\&quot;: \\&quot;10000\\&quot;, \\&quot;split_num\\&quot;: 3}&quot;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><ul><li><code>name</code>: 用于分片的列(只能有一列), 只支持 int8, int16, int32, int64类型的列</li><li><code>lower_bound</code>: 要读取列的最小值（若不设置, 则通过扫表获取）</li><li><code>upper_bound</code>: 要读取列的最大值（若不设置, 则通过扫表获取）</li><li><code>split_num</code>: 分片数量（若不设置，则与读并发一致）</li></ul><p>SIMPLE_DIVIDE分片策略将lower_bound和upper_bound之间的范围均分成split_num份，每一份即为一个分片。</p><hr><h2 id="kudu写入" tabindex="-1"><a class="header-anchor" href="#kudu写入" aria-hidden="true">#</a> Kudu写入</h2><h3 id="支持的数据类型" tabindex="-1"><a class="header-anchor" href="#支持的数据类型" aria-hidden="true">#</a> 支持的数据类型</h3><p>支持写入常见的Kudu数据类型:</p><ul><li>整型: <code>int8, int16, int32, int64</code>&#39;</li><li>浮点类型: <code>float, double, decimal</code></li><li>布尔类型: <code>boolean</code></li><li>日期类型: <code>date, timestamp</code></li><li>字符类型: <code>string, varchar</code></li><li>二进制类型: <code>binary, string_utf8</code></li></ul><h3 id="支持的操作类型" tabindex="-1"><a class="header-anchor" href="#支持的操作类型" aria-hidden="true">#</a> 支持的操作类型</h3><p>支持以下操作类型:</p><ul><li>INSERT, INSERT_IGNORE</li><li>UPSERT</li><li>UPDATE, UPDATE_IGNORE</li></ul><h3 id="主要参数-1" tabindex="-1"><a class="header-anchor" href="#主要参数-1" aria-hidden="true">#</a> 主要参数</h3><p>写连接器参数在<code>job.writer</code>中配置，实际使用时请注意路径前缀。示例:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.kudu.sink.KuduSink&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kudu_table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;kudu_test_table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kudu_master_address_list&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span><span class="token string">&quot;localhost:1234&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;localhost:4321&quot;</span><span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kudu_worker_count&quot;</span><span class="token operator">:</span> <span class="token number">2</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="必需参数-1" tabindex="-1"><a class="header-anchor" href="#必需参数-1" aria-hidden="true">#</a> 必需参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu写连接器类型, <code>com.bytedance.bitsail.connector.kudu.sink.KuduSink</code></td></tr><tr><td style="text-align:left;">kudu_table_name</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要写入的Kudu表</td></tr><tr><td style="text-align:left;">kudu_master_address_list</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu master地址, List形式表示</td></tr><tr><td style="text-align:left;">columns</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要写入的数据列的列名和类型</td></tr><tr><td style="text-align:left;">writer_parallelism_num</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">写并发</td></tr></tbody></table><h4 id="kuduclient相关参数-1" tabindex="-1"><a class="header-anchor" href="#kuduclient相关参数-1" aria-hidden="true">#</a> KuduClient相关参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">kudu_admin_operation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu client进行admin操作的timeout, 单位ms, 默认30000ms</td></tr><tr><td style="text-align:left;">kudu_operation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">Kudu client普通操作的timeout, 单位ms, 默认30000ms</td></tr><tr><td style="text-align:left;">kudu_connection_negotiation_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">单位ms，默认10000ms</td></tr><tr><td style="text-align:left;">kudu_disable_client_statistics</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否启用client段statistics统计</td></tr><tr><td style="text-align:left;">kudu_worker_count</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">client内worker数量</td></tr><tr><td style="text-align:left;">sasl_protocol_name</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">默认 &quot;kudu&quot;</td></tr><tr><td style="text-align:left;">require_authentication</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否开启鉴权</td></tr><tr><td style="text-align:left;">encryption_policy</td><td style="text-align:left;">否</td><td style="text-align:left;">OPTIONAL<br>REQUIRED_REMOTE<br>REQUIRED</td><td style="text-align:left;">加密策略</td></tr></tbody></table><h4 id="kudusession相关参数" tabindex="-1"><a class="header-anchor" href="#kudusession相关参数" aria-hidden="true">#</a> KuduSession相关参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必填</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数含义</th></tr></thead><tbody><tr><td style="text-align:left;">kudu_session_flush_mode</td><td style="text-align:left;">否</td><td style="text-align:left;">AUTO_FLUSH_SYNC<br>AUTO_FLUSH_BACKGROUND</td><td style="text-align:left;">session的flush模式, 默认AUTO_FLUSH_BACKGROUND</td></tr><tr><td style="text-align:left;">kudu_mutation_buffer_size</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">session最多能缓存多少条operation记录</td></tr><tr><td style="text-align:left;">kudu_session_flush_interval</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">session的flush间隔，单位ms</td></tr><tr><td style="text-align:left;">kudu_session_timeout_ms</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">session的operation超时</td></tr><tr><td style="text-align:left;">kudu_session_external_consistency_mode</td><td style="text-align:left;">否</td><td style="text-align:left;">CLIENT_PROPAGATED<br>COMMIT_WAIT</td><td style="text-align:left;">默认CLIENT_PROPAGATED</td></tr><tr><td style="text-align:left;">kudu_ignore_duplicate_rows</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">是否忽略因duplicate key造成的error, 默认false</td></tr></tbody></table><h2 id="相关文档" tabindex="-1"><a class="header-anchor" href="#相关文档" aria-hidden="true">#</a> 相关文档</h2>`,41);function g(f,y){const l=u("RouterLink");return d(),i("div",null,[c,e("p",null,[t("上级文档："),a(l,{to:"/zh/documents/connectors/"},{default:n(()=>[t("连接器")]),_:1})]),p,e("p",null,[t("配置示例文档："),a(l,{to:"/zh/documents/connectors/kudu/kudu-example.html"},{default:n(()=>[t("Kudu 连接器示例")]),_:1})])])}const _=s(r,[["render",g],["__file","kudu.html.vue"]]);export{_ as default};
