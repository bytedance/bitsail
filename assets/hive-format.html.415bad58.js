import{_ as d}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as i,c as n,a as e,b as o,d as a,w as r,e as l,r as c}from"./app.c1da3360.js";const s={},u=e("h1",{id:"bitsail-component-format-flink-hive",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-component-format-flink-hive","aria-hidden":"true"},"#"),o(" bitsail-component-format-flink-hive")],-1),m=e("hr",null,null,-1),v=l(`<p>This module provides <code>HiveGeneralRowBuilder</code> for supportinig converting hive <code>Writable</code> data into <code>Row</code>.</p><h2 id="how-to-use" tabindex="-1"><a class="header-anchor" href="#how-to-use" aria-hidden="true">#</a> how to use</h2><p>The working principle is to first obtain the meta information of the target hive table from the hive metastore, and then convert data according to the meta information <code>ObjectInspector</code>.</p><p>So we need two kinds of parameters to construct a <code>HiveGeneralRowBuilder</code>:</p><ol><li>Parameters for getting hive meta information: <ul><li><code>database</code>: hive database name</li><li><code>table</code>: hive table name</li><li><code>hiveProperties</code>: Properties of hive configuration to connect to hive metastore, which is stored as a Map.</li></ul></li><li><code>columnMapping</code>: The fields order of row to construct, which si stored as Map&lt;String, Integer&gt;. Map key is the field name, while value is the index of this field in hive table.</li></ol><h3 id="example" tabindex="-1"><a class="header-anchor" href="#example" aria-hidden="true">#</a> Example</h3><p>Take the following hive table <span id="jump_example_table"><code>test_db.test_table</code></span> as a example:</p><ul><li>Thrift uri for hive metastore is: <code>thrift://localhost:9083</code></li></ul><table><thead><tr><th>field name</th><th>field type</th></tr></thead><tbody><tr><td><code>id</code></td><td><code>BIGINT</code></td></tr><tr><td><code>state</code></td><td><code>STRING</code></td></tr><tr><td><code>county</code></td><td><code>STRING</code></td></tr></tbody></table><p>So we can use the following codes to construct a <code>HiveGeneralRowBuilder</code>:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="how-to-parse-writable" tabindex="-1"><a class="header-anchor" href="#how-to-parse-writable" aria-hidden="true">#</a> <span id="jump_parse">How to parse writable</span></h2><p>To parse <code>Writable</code> data, one needs <code>deserializer</code> and <code>ObjectInspector</code> information from hive table.</p><p><code>HiveGeneralRowBuilder</code> supports getting these meta information according to the hive information (including database, table, and some other properties used to connect to the metastore) passed in by the user.</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>// step1. Get hive meta info from metastore.
HiveMetaClientUtil.init();
HiveConf hiveConf = HiveMetaClientUtil.getHiveConf(hiveProperties);
StorageDescriptor storageDescriptor = HiveMetaClientUtil.getTableFormat(hiveConf, db, table);

// step2. Construct deserializer.
deserializer = (Deserializer) Class.forName(storageDescriptor.getSerdeInfo().getSerializationLib()).newInstance();
SerDeUtils.initializeSerDe(deserializer, conf, properties, null);

// step3. Construct \`ObjectInspector\`
structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
structFields = structObjectInspector.getAllStructFieldRefs();
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="how-to-convert-to-row" tabindex="-1"><a class="header-anchor" href="#how-to-convert-to-row" aria-hidden="true">#</a> How to Convert to Row</h2><p><code>HiveGeneralRowBuilder</code> implements the following interface to convert hive data to a <code>Row</code>:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>void build(Object objectValue, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException {
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><p>In this method, it has three steps:</p><ol><li><p>According to <a href="#jump_parse">&quot;How to parse writable&quot;</a>, it gets the <code>deseiralizer</code> and <code>structFields</code> for parsing.</p></li><li><p>According to <code>rowTypeInfo</code>, it extracts fields in order from <code>columnMapping</code> and <code>structField</code>. Based on these two information, it can extract the raw data.</p></li><li><p>According to the field type in <code>rowTypeInfo</code>, it converts the extracted raw data into <code>com.bytedance.bitsail.common.column.Column</code>, and then wraps it with <code>org.apache.flink.types.Row</code>.</p></li></ol><p>Take the above mentioned hive table <a href="#jump_example_table"><code>test_db.test_table</code></a> as an example, one can build <code>rowTypeInfo</code> and <code>columnMapping</code> as follows:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Using the above <code>rowTypeInfo</code> and <code>columnMapping</code>, one can get a <code>row</code> of field <code>id</code>, <code>state</code>, <code>county</code> by calling build method.</p><h2 id="supported-data-types" tabindex="-1"><a class="header-anchor" href="#supported-data-types" aria-hidden="true">#</a> Supported data types</h2><p><code>HiveGeneralRowBuiler</code> supports parsing common hive built-in data types, including all basic data types, and two complex data types, Map and List.</p><p>We support some types of data type conversion as follows:</p><table><thead><tr><th>Hive data type</th><th>You can convert the hive data type to</th><th>Description</th></tr></thead><tbody><tr><td>TINYINT<br>SMALLINT<br>INT<br>BIGINT</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code><br>3. <code>DoubleColumn</code><br></td><td>Take <code>1234L</code> as an example，the converted columns are:<br> 1. <code>StringColumn</code>: <code>&quot;1234&quot;</code><br>2. <code>LongColumn</code>: <code>1234</code><br>3. <code>DoubleColumn</code>: <code>1234.0</code></td></tr><tr><td>BOOLEAN</td><td>1. <code>StringColumn</code><br>2. <code>BooleanColumn</code></td><td>Take <code>false</code> as an example，the converted columns are:<br> 1. <code>StringColumn</code>: <code>&quot;false&quot;</code><br> 2. <code>BooleanColumn</code>: <code>false</code></td></tr><tr><td>FLOAT<br>DOUBLE<br>DECIMAL</td><td>1. <code>StringColumn</code><br>2. <code>DoubleColumn</code></td><td>Take <code>3.141592</code> as an example，the converted columns are:<br> 1. <code>StringColumn</code>: <code>&quot;3.141592&quot;</code><br> 2. <code>DoubleColumn</code>: <code>3.141592</code></td></tr><tr><td>STRING<br>CHAR<br>VARCHAR</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code><br>3. <code>DoubleColumn</code><br>4. <code>BooleanColumn</code><br>5. <code>DateColumn</code></td><td>1. <code>LongColumn</code>: Use <code>BigDecimal</code> to convert string to integer.<br>2. <code>DoubleColumn</code>: Use <code>Double.parseDouble</code> to convert string to float number<br>3. <code>BooleanColumn</code>: Only recognize<code>&quot;0&quot;, &quot;1&quot;, &quot;true&quot;, &quot;false&quot;</code></td></tr><tr><td>BINARY</td><td>1. <code>StringColumn</code><br>2. <code>BytesColumn</code></td><td>Take <code>byte[]{1, 2, 3}</code> as an example，the converted columns are:<br> 1. <code>StringColumn</code>: <code>&quot;[B@1d29cf23&quot;</code><br> 2. <code>BytesColumn</code>: <code>AQID</code></td></tr><tr><td>TIMESTAMP</td><td>1. <code>StringColumn</code><br>2. <code>LongColumn</code></td><td>Take <code>2022-01-01 10:00:00</code> as an example，the converted columns are:<br>1. <code>StringColumn</code>: <code>&quot;2022-01-01 10:00:00&quot;</code><br>2. <code>LongColumn</code>: <code>1641002400</code></td></tr><tr><td>DATE</td><td>1. <code>StringColumn</code><br> 2. <code>DateColumn</code> 3. <code>LongColumn</code></td><td>Take <code>2022-01-01</code> as example，the converted columns are:<br>1. <code>StringColumn</code>: <code>&quot;2022-01-01&quot;</code><br>2. <code>DateColumn</code>: <code>2022-01-01</code><br>3. <code>LongColumn</code>: <code>1640966400</code></td></tr></tbody></table><h2 id="example-1" tabindex="-1"><a class="header-anchor" href="#example-1" aria-hidden="true">#</a> Example</h2><p>Take the above mentioned hive table <a href="#jump_example_table"><code>test_db.test_table</code></a> as an example，the following codes show how to convert the <code>Writable</code> data in thie hive table to <code>Row</code>.</p><table><thead><tr><th>field name</th><th>field type</th></tr></thead><tbody><tr><td><code>id</code></td><td><code>BIGINT</code></td></tr><tr><td><code>state</code></td><td><code>STRING</code></td></tr><tr><td><code>county</code></td><td><code>STRING</code></td></tr></tbody></table><ul><li>Thrift uri of metastore: <code>thrift://localhost:9083</code></li></ul><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>/**
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,32);function b(p,h){const t=c("RouterLink");return i(),n("div",null,[u,m,e("p",null,[o("Parent document: "),a(t,{to:"/en/documents/components/format/introduction.html"},{default:r(()=>[o("bitsail-component-format-flink")]),_:1})]),v])}const w=d(s,[["render",b],["__file","hive-format.html.vue"]]);export{w as default};
