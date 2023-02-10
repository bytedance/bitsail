import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as e,a as s,b as n,d as p,w as c,e as l,r as u}from"./app.982a74e2.js";const i={},r=s("h1",{id:"ftp-sftp-连接器示例",tabindex:"-1"},[s("a",{class:"header-anchor",href:"#ftp-sftp-连接器示例","aria-hidden":"true"},"#"),n(" FTP/SFTP 连接器示例")],-1),d=l(`<p>下面展示了如何使用用户参数配置读取如下 CSV 格式文件。</p><ul><li>示例 CSV 数据</li></ul><div class="language-csv line-numbers-mode" data-ext="csv"><pre class="language-csv"><code><span class="token value">c1</span><span class="token punctuation">,</span><span class="token value">c2</span>
<span class="token value">aaa</span><span class="token punctuation">,</span><span class="token value">bbb</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><ul><li>用于读取上述格式文件的配置</li></ul><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.ftp.source.FtpInputFormat&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;protocol&quot;</span><span class="token operator">:</span><span class="token string">&quot;FTP&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;host&quot;</span><span class="token operator">:</span> <span class="token string">&quot;localhost&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;port&quot;</span><span class="token operator">:</span> <span class="token number">21</span><span class="token punctuation">,</span>
      <span class="token property">&quot;user&quot;</span><span class="token operator">:</span> <span class="token string">&quot;user&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;password&quot;</span><span class="token operator">:</span> <span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;path_list&quot;</span><span class="token operator">:</span> <span class="token string">&quot;/upload/&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;success_file_path&quot;</span><span class="token operator">:</span> <span class="token string">&quot;/upload/_SUCCESS&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;content_type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;csv&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;c1&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;c2&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,5);function k(v,q){const a=u("RouterLink");return o(),e("div",null,[r,s("p",null,[n("上级文档："),p(a,{to:"/zh/documents/connectors/ftp/ftp.html"},{default:c(()=>[n("FTP/SFTP 连接器")]),_:1})]),d])}const _=t(i,[["render",k],["__file","ftp-example.html.vue"]]);export{_ as default};
