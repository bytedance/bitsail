import{_ as d}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as n,c as r,a as e,b as t,d as a,w as i,e as s,r as o}from"./app.ea0662f4.js";const f={},u=e("h1",{id:"streamingfile-connector",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#streamingfile-connector","aria-hidden":"true"},"#"),t(" StreamingFile connector")],-1),y=s('<p><em>StreamingFile</em> Connector mainly used in streaming, and it supports write both hdfs and use hive <code>Exactly-Once</code> semantics. Provide reliable guarantee for real-time data warehouses.</p><h2 id="feature" tabindex="-1"><a class="header-anchor" href="#feature" aria-hidden="true">#</a> Feature</h2><ul><li>Support <code>Exactly Once</code>。</li><li>Support multi committer, compatible difference situations like data integrity or high timeliness.</li><li>Data Trigger, effectively solve problems like delayed data or out of order .</li><li>Hive DDL automatic detection, reduce manually align schema with hive.</li></ul><h2 id="support-data-types" tabindex="-1"><a class="header-anchor" href="#support-data-types" aria-hidden="true">#</a> Support data types</h2><ul><li>HDFS <ul><li>No need to care about the data types; write byte array directly.</li></ul></li><li>HIVE <ul><li>Basic data types. <ul><li>TINYINT</li><li>SMALLINT</li><li>INT</li><li>BIGINT</li><li>BOOLEAN</li><li>FLOAT</li><li>DOUBLE</li><li>STRING</li><li>BINARY</li><li>TIMESTAMP</li><li>DECIMAL</li><li>CHAR</li><li>VARCHAR</li><li>DATE</li></ul></li><li>Complex data types. <ul><li>Array</li><li>Map</li></ul></li></ul></li></ul><h2 id="parameters" tabindex="-1"><a class="header-anchor" href="#parameters" aria-hidden="true">#</a> Parameters</h2><h3 id="common-parameters" tabindex="-1"><a class="header-anchor" href="#common-parameters" aria-hidden="true">#</a> Common Parameters</h3><table><thead><tr><th style="text-align:left;">Name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Default Value</th><th style="text-align:left;">Enumeration Value</th><th style="text-align:left;">Comments</th></tr></thead><tbody><tr><td style="text-align:left;">class</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">com.bytedance.bitsail.connector.legacy.streamingfile.sink.FileSystemSinkFunctionDAGBuilder</td></tr><tr><td style="text-align:left;">dump.format.type</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;">hdfs<br>hive<br></td><td style="text-align:left;">Write <code>hdfs</code> or <code>hive</code></td></tr></tbody></table><h3 id="advanced-parameters" tabindex="-1"><a class="header-anchor" href="#advanced-parameters" aria-hidden="true">#</a> Advanced Parameters</h3><table><thead><tr><th style="text-align:left;">Name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Default Value</th><th style="text-align:left;">Enumeration Value</th><th style="text-align:left;">Comments</th></tr></thead><tbody><tr><td style="text-align:left;">enable_event_time</td><td style="text-align:left;">No</td><td style="text-align:left;">False</td><td style="text-align:left;"></td><td style="text-align:left;">Enable event time or not.</td></tr><tr><td style="text-align:left;">event_time_fields</td><td style="text-align:left;">No</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">If enable event time, use this parameter to specify the field name in the record.</td></tr><tr><td style="text-align:left;">event_time_pattern</td><td style="text-align:left;">No</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">If enable event time，if this parameter is null then use unix timestamp to parse the <code>event_time_fields</code>. If this field is not empty, use this field&#39;s value to parse the field value, examples: &quot;yyyy-MM-dd HH:mm:ss&quot;</td></tr><tr><td style="text-align:left;">event_time.tag_duration</td><td style="text-align:left;">No</td><td style="text-align:left;">900000</td><td style="text-align:left;"></td><td style="text-align:left;">Unit:millisecond. Maximum wait time for the event time trigger. The formula: event_time - pending_commit_time &gt; event_time.tag_duration, then will trigger the event time.Example: current event time=9：45, tag_duration=40min, pending trigger_commit_time=8:00, then 9:45 - (8:00 + 60min) = 45min &gt; 40min the result is true, then event time could be trigger.</td></tr><tr><td style="text-align:left;">dump.directory_frequency</td><td style="text-align:left;">No</td><td style="text-align:left;">dump.directory_frequency.day</td><td style="text-align:left;">dump.directory_frequency.day<br>dump.directory_frequency.hour</td><td style="text-align:left;">Use for write hdfs.<br> dump.directory_frequency.day:/202201/xx_data<br> dump.directory_frequency.hour: /202201/01/data</td></tr><tr><td style="text-align:left;">rolling.inactivity_interval</td><td style="text-align:left;">No</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">The interval of the file rolling.</td></tr><tr><td style="text-align:left;">rolling.max_part_size</td><td style="text-align:left;">No</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">The file size of the file rolling.</td></tr><tr><td style="text-align:left;">partition_strategy</td><td style="text-align:left;">No</td><td style="text-align:left;">partition_last</td><td style="text-align:left;">partition_first<br>partition_last</td><td style="text-align:left;">Committer strategy. partition_last: Waiting for all data ready then add hive partition to metastore.partition_first：add partition first。</td></tr></tbody></table><h3 id="hdfs-parameters" tabindex="-1"><a class="header-anchor" href="#hdfs-parameters" aria-hidden="true">#</a> HDFS Parameters</h3><table><thead><tr><th style="text-align:left;">Name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Default Value</th><th style="text-align:left;">Enumeration Value</th><th style="text-align:left;">Comments</th></tr></thead><tbody><tr><td style="text-align:left;">dump.output_dir</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">The location of hdfs output.</td></tr><tr><td style="text-align:left;">hdfs.dump_type</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;">hdfs.dump_type.text<br>hdfs.dump_type.json<br>hdfs.dump_type.msgpack<br>hdfs.dump_type.binary: <code>protobuf record, need use with follow parameters, proto.descriptor and proto.class_name</code>.</td><td style="text-align:left;">How the parse the record for the event_time</td></tr><tr><td style="text-align:left;">partition_infos</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">The partition for the hdfs directory, hdfs only can be the follow value [{&quot;name&quot;:&quot;date&quot;,&quot;value&quot;:&quot;yyyyMMdd&quot;,&quot;type&quot;:&quot;TIME&quot;},{&quot;name&quot;:&quot;hour&quot;,&quot;value&quot;:&quot;HH&quot;,&quot;type&quot;:&quot;TIME&quot;}]</td></tr><tr><td style="text-align:left;">hdfs.replication</td><td style="text-align:left;">No</td><td style="text-align:left;">3</td><td style="text-align:left;"></td><td style="text-align:left;">hdfs replication num.</td></tr><tr><td style="text-align:left;">hdfs.compression_codec</td><td style="text-align:left;">No</td><td style="text-align:left;">None</td><td style="text-align:left;"></td><td style="text-align:left;">hdfs file compression strategy.</td></tr></tbody></table><h3 id="hive-parameters" tabindex="-1"><a class="header-anchor" href="#hive-parameters" aria-hidden="true">#</a> Hive Parameters</h3><table><thead><tr><th style="text-align:left;">Name</th><th style="text-align:left;">Required</th><th style="text-align:left;">Default Value</th><th style="text-align:left;">Enumeration Value</th><th style="text-align:left;">Comments</th></tr></thead><tbody><tr><td style="text-align:left;">db_name</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Database name for hive.</td></tr><tr><td style="text-align:left;">table_name</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Table name for hive.</td></tr><tr><td style="text-align:left;">metastore_properties</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Hive metastore configuration. eg: {&quot;metastore_uris&quot;:&quot;thrift:localhost:9083&quot;}</td></tr><tr><td style="text-align:left;">source_schema</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Source schema, eg: [{&quot;name&quot;:&quot;id&quot;,&quot;type&quot;:&quot;bigint&quot;},{&quot;name&quot;:&quot;user_name&quot;,&quot;type&quot;:&quot;string&quot;},{&quot;name&quot;:&quot;create_time&quot;,&quot;type&quot;:&quot;bigint&quot;}]</td></tr><tr><td style="text-align:left;">sink_schema</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Sink schema, eg: [{&quot;name&quot;:&quot;id&quot;,&quot;type&quot;:&quot;bigint&quot;},{&quot;name&quot;:&quot;user_name&quot;,&quot;type&quot;:&quot;string&quot;},{&quot;name&quot;:&quot;create_time&quot;,&quot;type&quot;:&quot;bigint&quot;}]</td></tr><tr><td style="text-align:left;">partition_infos</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;"></td><td style="text-align:left;">Hive partition definition, eg: [{&quot;name&quot;:&quot;date&quot;,&quot;type&quot;:&quot;TIME&quot;},{&quot;name&quot;:&quot;hour&quot;,&quot;type&quot;:&quot;TIME&quot;}]</td></tr><tr><td style="text-align:left;">hdfs.dump_type</td><td style="text-align:left;">Yes</td><td style="text-align:left;">-</td><td style="text-align:left;">hdfs.dump_type.text<br>hdfs.dump_type.json<br>hdfs.dump_type.msgpack<br>hdfs.dump_type.binary: protobuf record, need use with follow parameter, proto.descriptor and proto.class_name。</td><td style="text-align:left;"></td></tr></tbody></table><h2 id="reference-docs" tabindex="-1"><a class="header-anchor" href="#reference-docs" aria-hidden="true">#</a> Reference docs</h2>',15);function h(g,m){const l=o("RouterLink");return n(),r("div",null,[u,e("p",null,[t("Parent document: "),a(l,{to:"/en/documents/connectors/"},{default:i(()=>[t("Connectors")]),_:1})]),y,e("p",null,[t("Configuration examples: "),a(l,{to:"/en/documents/connectors/streamingfile/streamingfile_example.html"},{default:i(()=>[t("StreamingFile connector example")]),_:1})])])}const p=d(f,[["render",h],["__file","streamingfile.html.vue"]]);export{p as default};