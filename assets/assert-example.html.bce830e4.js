import{_ as e}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as t,c as o,a as n,b as s,d as p,w as c,e as l,r}from"./app.fe93ffff.js";const i={},u=n("h1",{id:"assert-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#assert-connector-example","aria-hidden":"true"},"#"),s(" Assert connector example")],-1),d=l(`<p>The exact meaning of the following configuration.</p><ul><li>Total number of data entries in the range <code>[10, 20]</code></li><li>The <code>name</code> column is not-null and has a string length in the range <code>[1, 1000]</code></li><li>The <code>price</code> column is not-null and has a value in the range <code>[2, 180]</code></li></ul><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.assertion.sink.AssertSink&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;price&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;row_rules&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;min_row&quot;</span><span class="token operator">:</span> <span class="token number">10</span><span class="token punctuation">,</span>
        <span class="token property">&quot;max_row&quot;</span><span class="token operator">:</span> <span class="token number">20</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;column_rules&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
        <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;not_null&quot;</span><span class="token operator">:</span> <span class="token boolean">true</span><span class="token punctuation">,</span>
          <span class="token property">&quot;min_len&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
          <span class="token property">&quot;max_len&quot;</span><span class="token operator">:</span> <span class="token number">1000</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token property">&quot;price&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;not_null&quot;</span><span class="token operator">:</span> <span class="token boolean">true</span><span class="token punctuation">,</span>
          <span class="token property">&quot;min&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
          <span class="token property">&quot;max&quot;</span><span class="token operator">:</span> <span class="token number">180</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,3);function k(m,v){const a=r("RouterLink");return t(),o("div",null,[u,n("p",null,[s("Parent document: "),p(a,{to:"/en/documents/connectors/assert/assert.html"},{default:c(()=>[s("Assert connector")]),_:1})]),d])}const _=e(i,[["render",k],["__file","assert-example.html.vue"]]);export{_ as default};
