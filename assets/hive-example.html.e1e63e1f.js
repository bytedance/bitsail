import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as e,c as o,a as n,b as s,d as p,w as i,e as l,r as u}from"./app.adeb8394.js";const c={},r=n("h1",{id:"hive-连接器示例",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#hive-连接器示例","aria-hidden":"true"},"#"),s(" Hive 连接器示例")],-1),d=l(`<p>下面展示如何使用用户参数配置读取测试hive表:</p><h2 id="测试hive表信息" tabindex="-1"><a class="header-anchor" href="#测试hive表信息" aria-hidden="true">#</a> 测试hive表信息</h2><ul><li>示例hive信息： <ul><li><p>hive库名: test_db</p></li><li><p>hive表名: test_table</p></li><li><p>metastore uri地址: <code>thrift://localhost:9083</code></p></li><li><p>分区: p_date</p></li><li><p>表结构:</p><table><thead><tr><th>字段名</th><th>字段类型</th><th>说明</th></tr></thead><tbody><tr><td>id</td><td>bigint</td><td></td></tr><tr><td>state</td><td>string</td><td></td></tr><tr><td>county</td><td>string</td><td></td></tr><tr><td>p_date</td><td>string</td><td>分区字段</td></tr></tbody></table></li></ul></li></ul><h2 id="hive-读连接器" tabindex="-1"><a class="header-anchor" href="#hive-读连接器" aria-hidden="true">#</a> Hive 读连接器</h2><p>读取上述测试hive表的用户配置:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
   <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
         <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hive.source.HiveInputFormat&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;state&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;county&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
            <span class="token punctuation">}</span>
         <span class="token punctuation">]</span><span class="token punctuation">,</span>
         <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_table&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;metastore_properties&quot;</span><span class="token operator">:</span> <span class="token string">&quot;{\\&quot;hive.metastore.uris\\&quot;:\\&quot;thrift://localhost:9083\\&quot;}&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;partition&quot;</span><span class="token operator">:</span> <span class="token string">&quot;p_date=20220101&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;reader_parallelism_num&quot;</span><span class="token operator">:</span> <span class="token number">1</span>
      <span class="token punctuation">}</span>
   <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="hive-写连接器" tabindex="-1"><a class="header-anchor" href="#hive-写连接器" aria-hidden="true">#</a> Hive 写连接器</h2><p>写入上述测试 Hive 表的用户配置:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
   <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
         <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hive.sink.HiveOutputFormat&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;state&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
               <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;county&quot;</span><span class="token punctuation">,</span>
               <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
            <span class="token punctuation">}</span>
         <span class="token punctuation">]</span><span class="token punctuation">,</span>
         <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_table&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;metastore_properties&quot;</span><span class="token operator">:</span> <span class="token string">&quot;{\\&quot;hive.metastore.uris\\&quot;:\\&quot;thrift://localhost:9083\\&quot;}&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;partition&quot;</span><span class="token operator">:</span> <span class="token string">&quot;p_date=20220101&quot;</span><span class="token punctuation">,</span>
         <span class="token property">&quot;writer_parallelism_num&quot;</span><span class="token operator">:</span> <span class="token number">1</span>
      <span class="token punctuation">}</span>
   <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,9);function v(k,q){const a=u("RouterLink");return e(),o("div",null,[r,n("p",null,[s("上级文档："),p(a,{to:"/zh/documents/connectors/hive/hive.html"},{default:i(()=>[s("Hive 连接器")]),_:1})]),d])}const h=t(c,[["render",v],["__file","hive-example.html.vue"]]);export{h as default};
