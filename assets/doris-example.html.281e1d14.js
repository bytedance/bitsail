import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as p,c as o,a as n,b as s,d as e,w as c,e as i,r as l}from"./app.de9af28d.js";const u={},r=n("h1",{id:"doris-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#doris-connector-example","aria-hidden":"true"},"#"),s(" Doris connector example")],-1),k=i(`<h2 id="doris-cluster-info" tabindex="-1"><a class="header-anchor" href="#doris-cluster-info" aria-hidden="true">#</a> Doris cluster info</h2><p>Assuming the doris</p><ul><li>fe: <code>127.0.0.1:1234</code></li><li>jdbc query地址: <code>127.0.0.1:4321</code></li></ul><p>Account:</p><ul><li>User: <code>test_user</code></li><li>Password: <code>1234567</code></li></ul><p>Target database and table:</p><ul><li>Database:: test_db</li><li>Table: test_doris_table</li></ul><p>DDL statement is:</p><div class="language-sql line-numbers-mode" data-ext="sql"><pre class="language-sql"><code><span class="token keyword">CREATE</span> <span class="token keyword">TABLE</span> <span class="token identifier"><span class="token punctuation">\`</span>test_db<span class="token punctuation">\`</span></span><span class="token punctuation">.</span><span class="token identifier"><span class="token punctuation">\`</span>test_doris_table<span class="token punctuation">\`</span></span> <span class="token punctuation">(</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>id<span class="token punctuation">\`</span></span> <span class="token keyword">bigint</span><span class="token punctuation">(</span><span class="token number">20</span><span class="token punctuation">)</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>bigint_type<span class="token punctuation">\`</span></span> <span class="token keyword">bigint</span><span class="token punctuation">(</span><span class="token number">20</span><span class="token punctuation">)</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>string_type<span class="token punctuation">\`</span></span> <span class="token keyword">varchar</span><span class="token punctuation">(</span><span class="token number">100</span><span class="token punctuation">)</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>double_type<span class="token punctuation">\`</span></span> <span class="token keyword">double</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>decimal_type<span class="token punctuation">\`</span></span> <span class="token keyword">decimal</span><span class="token punctuation">(</span><span class="token number">27</span><span class="token punctuation">,</span> <span class="token number">9</span><span class="token punctuation">)</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span> 
    <span class="token identifier"><span class="token punctuation">\`</span>date_type<span class="token punctuation">\`</span></span> <span class="token keyword">date</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span><span class="token punctuation">,</span>
    <span class="token identifier"><span class="token punctuation">\`</span>partition_date<span class="token punctuation">\`</span></span> <span class="token keyword">date</span> <span class="token boolean">NULL</span> <span class="token keyword">COMMENT</span> <span class="token string">&quot;&quot;</span> 
<span class="token punctuation">)</span> 
<span class="token keyword">ENGINE</span><span class="token operator">=</span>OLAP 
<span class="token keyword">DUPLICATE</span> <span class="token keyword">KEY</span><span class="token punctuation">(</span><span class="token identifier"><span class="token punctuation">\`</span>id<span class="token punctuation">\`</span></span><span class="token punctuation">)</span> 
<span class="token keyword">COMMENT</span> <span class="token string">&quot;OLAP&quot;</span> 
<span class="token keyword">PARTITION</span> <span class="token keyword">BY</span> RANGE<span class="token punctuation">(</span><span class="token identifier"><span class="token punctuation">\`</span>partition_date<span class="token punctuation">\`</span></span><span class="token punctuation">)</span> 
<span class="token punctuation">(</span>
    <span class="token keyword">PARTITION</span> p20221010 <span class="token keyword">VALUES</span> <span class="token punctuation">[</span><span class="token punctuation">(</span><span class="token string">&#39;2022-10-10&#39;</span><span class="token punctuation">)</span><span class="token punctuation">,</span> <span class="token punctuation">(</span><span class="token string">&#39;2022-10-11&#39;</span><span class="token punctuation">)</span><span class="token punctuation">)</span>
<span class="token punctuation">)</span> 
<span class="token keyword">DISTRIBUTED</span> <span class="token keyword">BY</span> <span class="token keyword">HASH</span><span class="token punctuation">(</span><span class="token identifier"><span class="token punctuation">\`</span>id<span class="token punctuation">\`</span></span><span class="token punctuation">)</span> BUCKETS <span class="token number">10</span> 
PROPERTIES 
<span class="token punctuation">(</span> 
    <span class="token string">&quot;replication_num&quot;</span> <span class="token operator">=</span> <span class="token string">&quot;3&quot;</span>
<span class="token punctuation">)</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="doris-writer" tabindex="-1"><a class="header-anchor" href="#doris-writer" aria-hidden="true">#</a> Doris writer</h2><p>You can use the following configuration to write data into <code>p20221010</code> partition of table <code>test_db.test_doris_table</code>.</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
   <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
     <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
       <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.doris.sink.DorisSink&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;fe_hosts&quot;</span><span class="token operator">:</span> <span class="token string">&quot;127.0.0.1:1234&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;mysql_hosts&quot;</span><span class="token operator">:</span> <span class="token string">&quot;127.0.0.1:4321&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;user&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_user&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;password&quot;</span><span class="token operator">:</span> <span class="token string">&quot;1234567&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_doris_table&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;partitions&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;p20221010&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;start_range&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span><span class="token string">&quot;2022-10-10&quot;</span><span class="token punctuation">]</span><span class="token punctuation">,</span>
           <span class="token property">&quot;end_range&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span><span class="token string">&quot;2022-10-11&quot;</span><span class="token punctuation">]</span>
         <span class="token punctuation">}</span>
       <span class="token punctuation">]</span><span class="token punctuation">,</span>
       <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint_type&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string_type&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;varchar&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">3</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double_type&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">4</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;decimal_type&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">5</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date_type&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">6</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;partition_date&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date&quot;</span>
         <span class="token punctuation">}</span>
       <span class="token punctuation">]</span>
     <span class="token punctuation">}</span>
   <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,12);function d(v,q){const a=l("RouterLink");return p(),o("div",null,[r,n("p",null,[s("Parent documents: "),e(a,{to:"/en/documents/connectors/doris/doris.html"},{default:c(()=>[s("Doris connector")]),_:1})]),k])}const y=t(u,[["render",d],["__file","doris-example.html.vue"]]);export{y as default};
