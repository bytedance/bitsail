import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as e,a as n,b as s,d as p,w as c,e as i,r as l}from"./app.ea0662f4.js";const u={},r=n("h1",{id:"kafka-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#kafka-connector-example","aria-hidden":"true"},"#"),s(" Kafka connector example")],-1),k=i(`<h2 id="kafka-configuration" tabindex="-1"><a class="header-anchor" href="#kafka-configuration" aria-hidden="true">#</a> Kafka configuration</h2><p>Suppose the kafka configuration used by the test is as follows:</p><ul><li><code>bootstrap.servers</code>: PLAINTEXT://localhost:9092</li><li><code>topic</code>: test_topic</li><li><code>group_id</code>: test_consumer_group</li></ul><h2 id="kafka-reader-example" tabindex="-1"><a class="header-anchor" href="#kafka-reader-example" aria-hidden="true">#</a> Kafka reader example</h2><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
      <span class="token property">&quot;connector&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;connector&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
          <span class="token property">&quot;bootstrap.servers&quot;</span><span class="token operator">:</span><span class="token string">&quot;PLAINTEXT://localhost:9092&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;topic&quot;</span><span class="token operator">:</span><span class="token string">&quot;test_topic&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;startup-mode&quot;</span><span class="token operator">:</span><span class="token string">&quot;earliest-offset&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;group&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;id&quot;</span><span class="token operator">:</span><span class="token string">&quot;test_consumer_group&quot;</span>
          <span class="token punctuation">}</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">}</span><span class="token punctuation">,</span>
      <span class="token property">&quot;child_connector_type&quot;</span><span class="token operator">:</span><span class="token string">&quot;kafka&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;format_type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;json&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;long&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;state&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;county&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span><span class="token punctuation">,</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.kafka.source.KafkaSourceFunctionDAGBuilder&quot;</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="kafka-writer-example" tabindex="-1"><a class="header-anchor" href="#kafka-writer-example" aria-hidden="true">#</a> Kafka writer example</h2><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
  <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
    <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
      <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.kafka.sink.KafkaOutputFormat&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;kafka_servers&quot;</span><span class="token operator">:</span> <span class="token string">&quot;PLAINTEXT://localhost:9092&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;topic_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_topic&quot;</span><span class="token punctuation">,</span>
      <span class="token property">&quot;writer_parallelism_num&quot;</span><span class="token operator">:</span> <span class="token number">3</span><span class="token punctuation">,</span>
      <span class="token property">&quot;log_failures_only&quot;</span><span class="token operator">:</span> <span class="token boolean">true</span><span class="token punctuation">,</span>
      <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;long&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;state&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span><span class="token punctuation">,</span>
        <span class="token punctuation">{</span>
          <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;county&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
        <span class="token punctuation">}</span>
      <span class="token punctuation">]</span>
    <span class="token punctuation">}</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,7);function d(v,q){const a=l("RouterLink");return o(),e("div",null,[r,n("p",null,[s("Parent document: "),p(a,{to:"/en/documents/connectors/kafka/kafka.html"},{default:c(()=>[s("Kafka connector")]),_:1})]),k])}const y=t(u,[["render",d],["__file","kafka-example.html.vue"]]);export{y as default};
