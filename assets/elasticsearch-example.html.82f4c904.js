import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as e,c as o,a as n,b as s,d as p,w as c,e as i,r as l}from"./app.a52a4427.js";const u={},r=n("h1",{id:"elasticsearch-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#elasticsearch-connector-example","aria-hidden":"true"},"#"),s(" Elasticsearch connector example")],-1),d=i(`<p>The following configuration shows how to organize parameter configuration to write the specified Elasticsearch index.</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,2);function k(v,m){const a=l("RouterLink");return e(),o("div",null,[r,n("p",null,[s("Parent document: "),p(a,{to:"/en/documents/connectors/elasticsearch/elasticsearch.html"},{default:c(()=>[s("Elasticsearch connector")]),_:1})]),d])}const _=t(u,[["render",k],["__file","elasticsearch-example.html.vue"]]);export{_ as default};
