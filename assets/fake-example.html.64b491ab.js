import{_ as e}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as t,c as o,a as n,b as a,d as p,w as c,e as r,r as l}from"./app.982a74e2.js";const i={},u=n("h1",{id:"fake-connector-examples",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#fake-connector-examples","aria-hidden":"true"},"#"),a(" Fake connector examples")],-1),d=r(`<hr><h2 id="fake-reader-example" tabindex="-1"><a class="header-anchor" href="#fake-reader-example" aria-hidden="true">#</a> Fake reader example</h2><p>Suppose you want to generate a data set of 300 records, and specify that each record has two fields named name and age, you can use the following configuration to read.</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.fake.source.FakeSource&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;total_count&quot;</span><span class="token operator">:</span> <span class="token number">300</span><span class="token punctuation">,</span>
      <span class="token property">&quot;rate&quot;</span><span class="token operator">:</span> <span class="token number">100</span><span class="token punctuation">,</span>
      <span class="token property">&quot;random_null_rate&quot;</span><span class="token operator">:</span> <span class="token number">0.1</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;age&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;int&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,4);function k(m,v){const s=l("RouterLink");return t(),o("div",null,[u,n("p",null,[a("Parent document: "),p(s,{to:"/en/documents/connectors/fake/fake.html"},{default:c(()=>[a("fake-connector")]),_:1})]),d])}const f=e(i,[["render",k],["__file","fake-example.html.vue"]]);export{f as default};
