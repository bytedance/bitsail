import{_ as e}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as t,c as o,a as n,b as s,d as p,w as r,e as i,r as c}from"./app.056e7792.js";const l={},u=n("h1",{id:"print-v1-连接器配置示例",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#print-v1-连接器配置示例","aria-hidden":"true"},"#"),s(" Print-V1 连接器配置示例")],-1),d=i(`<hr><h2 id="print写连接器" tabindex="-1"><a class="header-anchor" href="#print写连接器" aria-hidden="true">#</a> Print写连接器</h2><p>不管上游来的数据有多少个字段、是什么数据类型，都可以用如下配置将数据打印出来。</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.print.sink.PrintSink&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;sample_write&quot;</span><span class="token operator">:</span> <span class="token boolean">true</span><span class="token punctuation">,</span>
      <span class="token property">&quot;sample_limit&quot;</span><span class="token operator">:</span> <span class="token number">10</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,4);function m(v,k){const a=c("RouterLink");return t(),o("div",null,[u,n("p",null,[s("父目录: "),p(a,{to:"/zh/documents/connectors/print/v1/print-v1.html"},{default:r(()=>[s("print-connector")]),_:1})]),d])}const b=e(l,[["render",m],["__file","print-v1-example.html.vue"]]);export{b as default};
