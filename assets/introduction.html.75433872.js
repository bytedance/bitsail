import{_ as i}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as a,a as e,b as n,d as t,w as r,e as d,r as l}from"./app.fd52cdb7.js";const c={},u=e("h1",{id:"bitsail-flink-row-parser",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-flink-row-parser","aria-hidden":"true"},"#"),n(" bitsail-flink-row-parser")],-1),p=e("hr",null,null,-1),v=d(`<h2 id="内容" tabindex="-1"><a class="header-anchor" href="#内容" aria-hidden="true">#</a> 内容</h2><p>开发者在处理数据时，经常需要处理并解析bytes数据。本模块提供了数种格式的parser用于解析bytes数据。</p><table><thead><tr><th>类名</th><th>支持的格式</th><th>链接</th></tr></thead><tbody><tr><td><code>CsvBytesParser</code></td><td>CSV</td><td><a href="#jump_csv">link</a></td></tr><tr><td><code>JsonBytesParser</code></td><td>JSON</td><td><a href="#jump_json">link</a></td></tr><tr><td><code>PbBytesParser</code></td><td>Protobuf</td><td><a href="#jump_protobuf">link</a></td></tr></tbody></table><h3 id="csvbytesparser" tabindex="-1"><a class="header-anchor" href="#csvbytesparser" aria-hidden="true">#</a> <span id="jump_csv">CsvBytesParser</span></h3><p><code>CsvBytesParser</code>使用<code>org.apache.commons.csvCSVFormat</code>来解析csv格式的字符串，并支持以下参数:</p><ul><li><code>job.common.csv_delimiter</code>: 可通过此参数来配置分隔符，默认为 <code>,</code>。</li><li><code>job.common.csv_escape</code>: 可通过此参数来配置escape字符，默认不设置。</li><li><code>job.common.csv_quote</code>: 可通过此参数来配置quote字符，默认不设置。</li><li><code>job.common.csv_with_null_string</code>: 可通过此参数来配置null数据的转化值，默认不转化。</li></ul><h4 id="示例代码" tabindex="-1"><a class="header-anchor" href="#示例代码" aria-hidden="true">#</a> 示例代码</h4><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>public static void main(String[] args) throws Exception {
    String line = &quot;123,test_string,3.14&quot;;
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
      PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO
    );

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(RowParserOptions.CSV_DELIMITER, &quot;,&quot;);
    jobConf.set(RowParserOptions.CSV_QUOTE, &#39;&quot;&#39;);
    jobConf.set(RowParserOptions.CSV_WITH_NULL_STRING, &quot;null&quot;);

    CsvBytesParser parser = new CsvBytesParser(jobConf);

    Row row = new Row(3);
    byte[] bytes = line.getBytes();
    parser.parse(row, bytes, 0, bytes.length, &quot;UTF-8&quot;, rowTypeInfo);
    System.out.println(row);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="jsonbytesparser" tabindex="-1"><a class="header-anchor" href="#jsonbytesparser" aria-hidden="true">#</a> <span id="jump_json">JsonBytesParser</span></h3><p><code>JsonBytesParser</code>使用<code>com.bytedance.bitsail.common.util.FastJsonUtil</code>来解析json格式的字符串，并支持以下参数:</p><ul><li><code>job.common.case_insensitive</code>: 可通过此参数来配置是否对key大小写敏感，默认为<code>true</code>。</li><li><code>job.common.json_serializer_features</code>: 可通过此参数来设置用于<code>FastJsonUtil</code>解析时的properties，格式为 <code>&#39;,&#39;</code> 分隔的字符串，例如: <code>&quot;QuoteFieldNames,WriteNullListAsEmpty&quot;</code>。</li><li><code>job.common.convert_error_column_as_null</code>: 可通过此参数来配置是否在字段转化报错时，将该字段设置为null，默认为<code>false</code>。</li></ul><h4 id="示例代码-1" tabindex="-1"><a class="header-anchor" href="#示例代码-1" aria-hidden="true">#</a> 示例代码</h4><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>public static void main(String[] args) {
    String line = &quot;{\\&quot;id\\&quot;:123, \\&quot;state\\&quot;:\\&quot;California\\&quot;, \\&quot;county\\&quot;:\\&quot;Los Angeles\\&quot;}&quot;;
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
      PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
    );

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(RowParserOptions.JSON_SERIALIZER_FEATURES, &quot;QuoteFieldNames&quot;);
    JsonBytesParser parser = new JsonBytesParser(jobConf);
    
    Row row = new Row(3);
    byte[] bytes = line.getBytes();
    parser.parse(row, bytes, 0, bytes.length, &quot;UTF-8&quot;, rowTypeInfo);
    System.out.println(row);
  }
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="pbbytesparser" tabindex="-1"><a class="header-anchor" href="#pbbytesparser" aria-hidden="true">#</a> <span id="jump_protobuf">PbBytesParser</span></h3><p><code>PbBytesParser</code>使用用户传入的protobuf描述文件来解析bytes数据，支持以下参数:</p><ul><li><code>job.common.proto.descriptor</code>: 此参数为必需参数，用base64方式存储protobuf descriptor。</li><li><code>job.common.proto.class_name</code>: 此参数为必需参数，指定protobuf描述文件中用于解析的类名。</li></ul><h4 id="示例代码-2" tabindex="-1"><a class="header-anchor" href="#示例代码-2" aria-hidden="true">#</a> 示例代码</h4><p>示例proto文件<code>test.proto</code>如下:</p><div class="language-protobuf line-numbers-mode" data-ext="protobuf"><pre class="language-protobuf"><code><span class="token keyword">syntax</span> <span class="token operator">=</span> <span class="token string">&quot;proto2&quot;</span><span class="token punctuation">;</span>

<span class="token keyword">message</span> <span class="token class-name">ProtoTest</span> <span class="token punctuation">{</span>
  <span class="token keyword">required</span> <span class="token builtin">string</span> stringRow <span class="token operator">=</span> <span class="token number">1</span><span class="token punctuation">;</span>
  <span class="token keyword">required</span> <span class="token builtin">float</span> floatRow <span class="token operator">=</span> <span class="token number">2</span><span class="token punctuation">;</span>
  <span class="token keyword">required</span> <span class="token builtin">int64</span> int64Row <span class="token operator">=</span> <span class="token number">3</span><span class="token punctuation">;</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>使用上面proto的<code>PbBytesParser</code>示例如下:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>private transient Descriptor descriptor = null;

public void parsePbData(byte[] pbData) throws Exception {
  byte[] descriptor = IOUtils.toByteArray(new File(&quot;test.proto&quot;).toURI());
  RowTypeInfo rowTypeInfo = new RowTypeInfo(
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO
  );
    
  BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
  jobConf.set(RowParserOptions.PROTO_DESCRIPTOR, new String(descriptor));
  jobConf.set(RowParserOptions.PROTO_CLASS_NAME, &quot;ProtoTest&quot;);
  PbBytesParser parser = new PbBytesParser(jobConf);

  Row row = new Row(3);
  parser.parse(row, pbData, 0, pbData.length, null, rowTypeInfo);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,21);function b(m,f){const s=l("RouterLink");return o(),a("div",null,[u,p,e("p",null,[n("上级文档: "),t(s,{to:"/zh/documents/components/"},{default:r(()=>[n("bitsail-components")]),_:1})]),v])}const y=i(c,[["render",b],["__file","introduction.html.vue"]]);export{y as default};
