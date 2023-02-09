import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as e,c as o,a as s,b as n,d as p,w as c,e as i,r as l}from"./app.de9af28d.js";const u={},r=s("h1",{id:"elasticsearch-连接器示例",tabindex:"-1"},[s("a",{class:"header-anchor",href:"#elasticsearch-连接器示例","aria-hidden":"true"},"#"),n(" Elasticsearch 连接器示例")],-1),d=i(`<p>如下展示了如何使用用户参数配置写入指定的elasticsearch索引。</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchSink&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;es_id_fields&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;es_index&quot;</span><span class="token operator">:</span> <span class="token string">&quot;es_index_test&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;es_hosts&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span><span class="token string">&quot;http://localhost:1234&quot;</span><span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;json_serializer_features&quot;</span><span class="token operator">:</span> <span class="token string">&quot;QuoteFieldNames,UseSingleQuotes&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;bulk_backoff_max_retry_count&quot;</span><span class="token operator">:</span> <span class="token number">10</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;int&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double_type&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;text_type&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;text&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint_type&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,2);function k(v,q){const a=l("RouterLink");return e(),o("div",null,[r,s("p",null,[n("上级文档："),p(a,{to:"/zh/documents/connectors/elasticsearch/elasticsearch.html"},{default:c(()=>[n("Elasticsearch 连接器")]),_:1})]),d])}const _=t(u,[["render",k],["__file","elasticsearch-example.html.vue"]]);export{_ as default};
