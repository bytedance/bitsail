import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as e,a as n,b as s,d as p,w as c,e as l,r as i}from"./app.416da474.js";const u={},r=n("h1",{id:"mongodb-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#mongodb-connector-example","aria-hidden":"true"},"#"),s(" MongoDB connector example")],-1),d=l(`<p>Suppose starting a local MongoDB with connection url <code>mongodb://localhost:1234</code>. We create a database <code>test_db</code> and a collection <code>test_collection</code> on it.</p><h2 id="mongodb-reader" tabindex="-1"><a class="header-anchor" href="#mongodb-reader" aria-hidden="true">#</a> MongoDB Reader</h2><p>If the documents contains (_id, string_field, int_field) these three fields, we can use the following configuration to read.</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.mongodb.source.MongoDBInputFormat&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;split_key&quot;</span><span class="token operator">:</span> <span class="token string">&quot;_id&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;host&quot;</span><span class="token operator">:</span> <span class="token string">&quot;localhost&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;port&quot;</span><span class="token operator">:</span> <span class="token number">1234</span><span class="token punctuation">,</span>
      <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;collection_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_collection&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;_id&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;objectid&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string_field&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;int_field&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;long&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;reader_parallelism_num&quot;</span><span class="token operator">:</span><span class="token number">1</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="mongodb-writer" tabindex="-1"><a class="header-anchor" href="#mongodb-writer" aria-hidden="true">#</a> MongoDB Writer</h2><p>If you want to write (id, string_field, integer_field) these three fields into document, then you can use the following configuration.</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
   <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
     <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
       <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.mongodb.sink.MongoDBOutputFormat&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;unique_key&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;client_mode&quot;</span><span class="token operator">:</span> <span class="token string">&quot;url&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;mongo_url&quot;</span><span class="token operator">:</span> <span class="token string">&quot;mongodb://localhost:1234/test_db&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_db&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;collection_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_collection&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;integer_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;long&quot;</span>
         <span class="token punctuation">}</span>
       <span class="token punctuation">]</span>
     <span class="token punctuation">}</span>
   <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,7);function k(v,m){const a=i("RouterLink");return o(),e("div",null,[r,n("p",null,[s("Parent document: "),p(a,{to:"/en/documents/connectors/mongodb/mongodb.html"},{default:c(()=>[s("MongoDB connector")]),_:1})]),d])}const g=t(u,[["render",k],["__file","mongodb-example.html.vue"]]);export{g as default};
