import{_ as i}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as t,c as n,a as e,b as d,d as l,w as c,e as r,r as a}from"./app.416da474.js";const s={},u=e("h1",{id:"bitsail-component-format-flink-hive",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-component-format-flink-hive","aria-hidden":"true"},"#"),d(" bitsail-component-format-flink-hive")],-1),v=e("hr",null,null,-1),m=r(`<p>本模块中的<code>HiveGeneralRowBuilder</code>支持将从hive读出的原始<code>Writable</code>格式数据转化为<code>bitsail row</code>。</p><h2 id="如何构造" tabindex="-1"><a class="header-anchor" href="#如何构造" aria-hidden="true">#</a> 如何构造</h2><p><code>HiveGeneralRowBuilder</code>的工作原理是先从hive metastore中获取指定hive表的元信息，然后根据元信息中的<code>ObjectInspector</code>来解析数据。</p><p>因此在构造方法中需要两种参数：</p><ol><li>用于获取hive元信息的参数 <ul><li><code>database</code>: hive库名</li><li><code>table</code>: hive表名</li><li><code>hiveProperties</code>: 以Map形式存储的用于连接metastore的properties</li></ul></li><li><code>columnMapping</code>: 构建的row中字段顺序，以Map&lt;String,Integer&gt;形式存储。key为row的最终字段名，value表示该字段在hive中的index。</li></ol><h3 id="构建示例" tabindex="-1"><a class="header-anchor" href="#构建示例" aria-hidden="true">#</a> 构建示例</h3><p>以下面的hive表 <span id="jump_example_table"><code>test_db.test_table</code></span> 为例:</p><ul><li>元数据存储的thrift URI为 <code>thrift://localhost:9083</code></li></ul><table><thead><tr><th>字段名</th><th>字段类型</th></tr></thead><tbody><tr><td><code>id</code></td><td><code>BIGINT</code></td></tr><tr><td><code>state</code></td><td><code>STRING</code></td></tr><tr><td><code>county</code></td><td><code>STRING</code></td></tr></tbody></table><p>则可使用如下方法构造<code>HiveGeneralRowBuilder</code>:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
  &quot;id&quot;, 0,
  &quot;state&quot;, 1,
  &quot;county&quot;, 2
);

RowBuilder rowBuilder = new HiveGeneralRowBuilder(
  columnMapping,
  &quot;test_db&quot;,
  &quot;test_table&quot;,
  ImmutableMap.of(&quot;metastore_uri&quot;, &quot;thrift://localhost:9083&quot;)
);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="如何解析writable" tabindex="-1"><a class="header-anchor" href="#如何解析writable" aria-hidden="true">#</a> <span id="jump_parse">如何解析Writable</span></h2><p>为了解析<code>Writable</code>数据，需要hive数据表中的<code>deserializer</code>信息和<code>ObjectInspector</code>等元信息。</p><p>本模块支持的HiveGeneralRowBuilder根据用户传入的hive信息（包括database、table、以及一些其他用于连接metastore的properties），获取到解析需要的元信息。</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>// step1. Get hive meta info from metastore.
HiveMetaClientUtil.init();
HiveConf hiveConf = HiveMetaClientUtil.getHiveConf(hiveProperties);
StorageDescriptor storageDescriptor = HiveMetaClientUtil.getTableFormat(hiveConf, db, table);

// step2. Construct deserializer.
deserializer = (Deserializer) Class.forName(storageDescriptor.getSerdeInfo().getSerializationLib()).newInstance();
SerDeUtils.initializeSerDe(deserializer, conf, properties, null);

// step3. Construct \`ObjectInspector\`
structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
structFields = structObjectInspector.getAllStructFieldRefs();
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="如何转化为row" tabindex="-1"><a class="header-anchor" href="#如何转化为row" aria-hidden="true">#</a> 如何转化为Row</h2><p><code>HiveGeneralRowBuilder</code>扩展了如下接口，用以解析并构建row。</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>void build(Object objectValue, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException {
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><p>在此方法中:</p><ol><li>按照<a href="#jump_parse">如何解析Writable</a>获取解析需要使用的<code>deseiralizer</code>和<code>structFields</code>。</li><li>按照<code>rowTypeInfo</code>，依次从<code>columnMapping</code>中获取字段在hive中的<code>structField</code>，并以此解析出原始数据。</li><li>根据<code>rowTypeInfo</code>中的数据类型将解析后的原始数据转化为相应的<code>Column</code>，再存储到<code>reuse</code>中。</li></ol><p>以上面提到的 <a href="#jump_example_table"><code>test_db.test_table</code></a> 为例，可按照如下方式构建<code>rowTypeInfo</code>和<code>columnMapping</code>:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

TypeInformation&lt;?&gt;[] fieldTypes = new TypeInformation[] {
  PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
  PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
  PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
};
RowTypeInfo rowTypeInfo = new RowTypeInfo(
  fieldTypes,
  new String[] {&quot;id_field&quot;, &quot;state_field&quot;, &quot;county_field&quot;}
);

Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
  &quot;id_field&quot;, 0,
  &quot;state_field&quot;, 1,
  &quot;county_field&quot;, 2
);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>用上面的<code>columnMapping</code>构建<code>HiveGeneralRowBuilder</code>和<code>rowTypeInfo</code>调用build方法后，即可获得内容按字段顺序为<code>id</code>, <code>state</code>, <code>county</code>的<code>row</code>。</p><h2 id="支持的数据类型" tabindex="-1"><a class="header-anchor" href="#支持的数据类型" aria-hidden="true">#</a> 支持的数据类型</h2><p><code>HiveGeneralRowBuiler</code>支持解析常见的Hive内置数据类型，包括所有的基础数据类型，以及Map和List两种复杂数据类型。</p><p>基础数据类型解析后以 <code>com.bytedance.bitsail.common.column.Column</code> 类型存储，并支持一定的数据类型转化，具体见下表:</p><table><thead><tr><th>hive数据类型</th><th>支持的转化类型</th><th>说明</th></tr></thead><tbody><tr><td>TINYINT<br>SMALLINT<br>INT<br>BIGINT</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code><br>3. <code>DoubleColumn</code><br></td><td>以<code>1234L</code>为例，转化后的数据分别为:<br> 1. <code>StringColumn</code>: <code>&quot;1234&quot;</code><br>2. <code>LongColumn</code>: <code>1234</code><br>3. <code>DoubleColumn</code>: <code>1234.0</code></td></tr><tr><td>BOOLEAN</td><td>1. <code>StringColumn</code><br>2. <code>BooleanColumn</code></td><td>以<code>false</code>为例，转化后的数据分别为:<br> 1. <code>StringColumn</code>: <code>&quot;false&quot;</code><br> 2. <code>BooleanColumn</code>: <code>false</code></td></tr><tr><td>FLOAT<br>DOUBLE<br>DECIMAL</td><td>1. <code>StringColumn</code><br>2. <code>DoubleColumn</code></td><td>以<code>3.141592</code>为例，转化后的数据分别为:<br> 1. <code>StringColumn</code>: <code>&quot;3.141592&quot;</code><br> 2. <code>DoubleColumn</code>: <code>3.141592</code></td></tr><tr><td>STRING<br>CHAR<br>VARCHAR</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code><br>3. <code>DoubleColumn</code><br>4. <code>BooleanColumn</code><br>5. <code>DateColumn</code></td><td>1. <code>LongColumn</code>: 使用<code>BigDecimal</code>来处理字符类型数据，字符串需要满足<code>BigDecimal</code>的格式需求。<br>2. <code>DoubleColumn</code>: 使用<code>Double.parseDouble</code>处理字符类型浮点数，字符串需要满足<code>Double</code>格式需求。<br>3. <code>BooleanColumn</code>: 仅识别字符串<code>&quot;0&quot;, &quot;1&quot;, &quot;true&quot;, &quot;false&quot;</code>。</td></tr><tr><td>BINARY</td><td>1. <code>StringColumn</code><br>2. <code>BytesColumn</code></td><td>以<code>byte[]{1, 2, 3}</code>为例，转化后的数据分别为:<br> 1. <code>StringColumn</code>: <code>&quot;[B@1d29cf23&quot;</code><br> 2. <code>BytesColumn</code>: <code>AQID</code></td></tr><tr><td>TIMESTAMP</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code></td><td>以 <code>2022-01-01 10:00:00</code>为例，转化后的数据分别为:<br>1. <code>StringColumn</code>: <code>&quot;2022-01-01 10:00:00&quot;</code><br>2. <code>LongColumn</code>: <code>1641002400</code></td></tr><tr><td>DATE</td><td>1. <code>StringColumn</code><br> 2. <code>DateColumn</code> 3. <code>LongColumn</code></td><td>以 <code>2022-01-01</code>为例，转化后的数据分别为:<br>1. <code>StringColumn</code>: <code>&quot;2022-01-01&quot;</code><br>2. <code>DateColumn</code>: <code>2022-01-01</code><br>3. <code>LongColumn</code>: <code>1640966400</code></td></tr></tbody></table><h2 id="使用示例" tabindex="-1"><a class="header-anchor" href="#使用示例" aria-hidden="true">#</a> 使用示例</h2><p>使用上面提到的<a href="#jump_example_table"><code>test_db.test_table</code></a>为例，下面的代码展示了如何转化这张hive表中读取的<code>Writable</code>数据为想要的<code>Row</code>数据。</p><table><thead><tr><th>字段名</th><th>字段类型</th></tr></thead><tbody><tr><td><code>id</code></td><td><code>BIGINT</code></td></tr><tr><td><code>state</code></td><td><code>STRING</code></td></tr><tr><td><code>county</code></td><td><code>STRING</code></td></tr></tbody></table><ul><li>元数据存储的thrift URI为 <code>thrift://localhost:9083</code></li></ul><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>/**
 * @param rawData Writable data for building a row.
 */
public Row buildRow(Writable rawData) {
  // 1. Initialize hive row builder
  String database = &quot;test_db&quot;;
  String table = &quot;test_table&quot;;
  Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
    &quot;id&quot;, 0,
    &quot;state&quot;, 1,
    &quot;county&quot;, 2
  );
  Map&lt;String, String&gt; hiveProperties = ImmutableMap.of(
    &quot;metastore_uri&quot;, &quot;thrift://localhost:9083&quot;
  );
  RowBuilder rowBuilde = new HiveGeneralRowBuilder(
    columnMapping, database, table, hiveProperties
  );

  // 2. Construct row type infomation.
  TypeInformation&lt;?&gt;[] typeInformationList = {
    PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
  };
  RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformationList,
    new String[] {&quot;id&quot;, &quot;state&quot;, &quot;county&quot;}
  );
  
  // 3. Parse rawData and build row.
  Row reuse = new Row(3);
  rowBuilder.build(rawData, reuse, &quot;UTF-8&quot;, rowTypeInfo);
  return reuse;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,32);function b(p,h){const o=a("RouterLink");return t(),n("div",null,[u,v,e("p",null,[d("上级文档: "),l(o,{to:"/zh/documents/components/format/introduction.html"},{default:c(()=>[d("bitsail-component-format-flink")]),_:1})]),m])}const I=i(s,[["render",b],["__file","hive-format.html.vue"]]);export{I as default};
