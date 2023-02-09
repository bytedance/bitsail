import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as p,a as n,b as s,d as e,w as u,e as i,r as c}from"./app.de9af28d.js";const l={},r=n("h1",{id:"hudi-连接器示例",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#hudi-连接器示例","aria-hidden":"true"},"#"),s(" Hudi 连接器示例")],-1),d=i(`<h2 id="hudi-读连接器" tabindex="-1"><a class="header-anchor" href="#hudi-读连接器" aria-hidden="true">#</a> Hudi 读连接器</h2><p>读取 Hudi 表的用户配置：</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="hudi-写连接器" tabindex="-1"><a class="header-anchor" href="#hudi-写连接器" aria-hidden="true">#</a> Hudi 写连接器</h2><p>写入hudi表的用户配置:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
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
      <span class="token property">&quot;format_type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;json&quot;</span><span class="token punctuation">,</span>
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="hudi-compaction示例" tabindex="-1"><a class="header-anchor" href="#hudi-compaction示例" aria-hidden="true">#</a> Hudi compaction示例</h2><p>实例参数用于压缩Hudi表:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span><span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.source.HudiCompactSourceDAGBuilder&quot;</span>
    <span class="token punctuation">}</span><span class="token punctuation">,</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;path&quot;</span><span class="token operator">:</span><span class="token string">&quot;/path/to/table&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.hudi.sink.HudiCompactSinkDAGBuilder&quot;</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,9);function k(q,v){const a=c("RouterLink");return o(),p("div",null,[r,n("p",null,[s("上级文档："),e(a,{to:"/zh/documents/connectors/hudi/hudi.html"},{default:u(()=>[s("Hudi 连接器")]),_:1})]),d])}const h=t(l,[["render",k],["__file","hudi-example.html.vue"]]);export{h as default};
