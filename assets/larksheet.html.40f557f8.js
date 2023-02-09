import{_ as p}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as i,c,a as t,b as n,d as a,w as s,e as d,r as l}from"./app.de9af28d.js";const r={},u=t("h1",{id:"larksheet-飞书表格-连接器",tabindex:"-1"},[t("a",{class:"header-anchor",href:"#larksheet-飞书表格-连接器","aria-hidden":"true"},"#"),n(" LarkSheet(飞书表格) 连接器")],-1),h=t("p",null,[t("strong",null,"BitSail"),n(" 飞书表格连接器可用于支持读取飞书表格，主要功能如下:")],-1),k=t("li",null,"支持批式一次读取多张飞书表格",-1),g={href:"https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology?lang=en-US",target:"_blank",rel:"noopener noreferrer"},m=t("li",null,"支持读取表格中的部分列",-1),b=d(`<h2 id="依赖引入" tabindex="-1"><a class="header-anchor" href="#依赖引入" aria-hidden="true">#</a> 依赖引入</h2><div class="language-xml line-numbers-mode" data-ext="xml"><pre class="language-xml"><code><span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>dependency</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>groupId</span><span class="token punctuation">&gt;</span></span>com.bytedance.bitsail<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>groupId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>artifactId</span><span class="token punctuation">&gt;</span></span>bitsail-connector-larksheet<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>artifactId</span><span class="token punctuation">&gt;</span></span>
   <span class="token tag"><span class="token tag"><span class="token punctuation">&lt;</span>version</span><span class="token punctuation">&gt;</span></span>\${revision}<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>version</span><span class="token punctuation">&gt;</span></span>
<span class="token tag"><span class="token tag"><span class="token punctuation">&lt;/</span>dependency</span><span class="token punctuation">&gt;</span></span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="飞书表格读取" tabindex="-1"><a class="header-anchor" href="#飞书表格读取" aria-hidden="true">#</a> 飞书表格读取</h3><h3 id="支持数据类型" tabindex="-1"><a class="header-anchor" href="#支持数据类型" aria-hidden="true">#</a> 支持数据类型</h3><p>飞书表格连接器以 <code>string</code> 格式读取所有数据。</p><h3 id="参数" tabindex="-1"><a class="header-anchor" href="#参数" aria-hidden="true">#</a> 参数</h3><p>读连接器参数在<code>job.reader</code>中配置，实际使用时请注意路径前缀。示例:</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;sheet_urls&quot;</span><span class="token operator">:</span> <span class="token string">&quot;https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=ZbzDHq&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;datetime&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="必需参数" tabindex="-1"><a class="header-anchor" href="#必需参数" aria-hidden="true">#</a> 必需参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必须</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数描述</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">飞书表格读连接器名, <code>com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat</code></td></tr><tr><td style="text-align:left;">sheet_urls</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">要读取的飞书表格列表。多个表格链接用英文逗号分隔。</td></tr><tr><td style="text-align:left;">columns</td><td style="text-align:left;">是</td><td style="text-align:left;"></td><td style="text-align:left;">描述字段名称和字段类型。字段名称与飞书表格中的header相关（header即为第一行）。</td></tr></tbody></table><p>下面的参数用于鉴权，用户至少需要设置 (<code>sheet_token</code>) 或者 (<code>app_id</code> and <code>app_secret</code>)其中一种。 |</p><table><tr><th>参数名称</th><th>是否必须</th><th>参数枚举值</th><th>参数描述</th></tr><tr><td>sheet_token</td><td rowspan="3">至少设置下述一项:<br>1. sheet_token<br> 2. app_id 和 app_secret</td><td></td><td>用于<a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">飞书 open api</a>鉴权的token.</td></tr><tr><td>app_id</td><td></td><td rowspan="2">使用 app_id 和 app_secret 来生成用于<a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">飞书 open api</a>鉴权的token.</td></tr><tr><td>app_secret</td><td></td></tr></table><p>注意，<code>sheet_token</code>可能在任务运行中过期。 如果使用<code>app_id</code> 和 <code>app_secret</code>，会主动刷新过期token。</p><h4 id="可选参数" tabindex="-1"><a class="header-anchor" href="#可选参数" aria-hidden="true">#</a> 可选参数</h4><table><thead><tr><th style="text-align:left;">参数名称</th><th style="text-align:left;">是否必须</th><th style="text-align:left;">参数枚举值</th><th style="text-align:left;">参数描述</th></tr></thead><tbody><tr><td style="text-align:left;">reader_parallelism_num</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">读并发</td></tr><tr><td style="text-align:left;">batch_size</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">从open api一次拉取的数据行数</td></tr><tr><td style="text-align:left;">skip_nums</td><td style="text-align:left;">否</td><td style="text-align:left;"></td><td style="text-align:left;">对于每个表格可指定跳过开头的行数。用list表示</td></tr></tbody></table><h2 id="相关文档" tabindex="-1"><a class="header-anchor" href="#相关文档" aria-hidden="true">#</a> 相关文档</h2>`,16);function f(v,x){const e=l("RouterLink"),o=l("ExternalLinkIcon");return i(),c("div",null,[u,t("p",null,[n("上级文档："),a(e,{to:"/zh/documents/connectors/"},{default:s(()=>[n("连接器")]),_:1})]),h,t("ul",null,[k,t("li",null,[n("支持token和 "),t("a",g,[n("application"),a(o)]),n(" 两种鉴权方式")]),m]),b,t("p",null,[n("配置示例文档："),a(e,{to:"/zh/documents/connectors/larksheet/larksheet-example.html"},{default:s(()=>[n("LarkSheet(飞书表格)连接器示例")]),_:1})])])}const q=p(r,[["render",f],["__file","larksheet.html.vue"]]);export{q as default};
