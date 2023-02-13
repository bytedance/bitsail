import{_ as l,a as r,b as t}from"./product_structure.fcab2163.js";import{_ as o}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as c,c as u,a as e,b as i,d as n,w as v,e as m,r as a}from"./app.7342ef47.js";const b={},h=e("h1",{id:"bitsail-guide-video",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-guide-video","aria-hidden":"true"},"#"),i(" BitSail Guide Video")],-1),p=e("hr",null,null,-1),_=e("h2",{id:"bitsail-demo-video",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-demo-video","aria-hidden":"true"},"#"),i(" BitSail demo video")],-1),g={href:"https://zhuanlan.zhihu.com/p/595157599",target:"_blank",rel:"noopener noreferrer"},f=m('<h2 id="bitsail-source-code-compilation" tabindex="-1"><a class="header-anchor" href="#bitsail-source-code-compilation" aria-hidden="true">#</a> BitSail source code compilation</h2><p>BitSail has a built-in compilation script <code>build.sh</code> in the project, which is stored in the project root directory. Newly downloaded users can directly compile this script, and after successful compilation, they can find the corresponding product in the directory: <code>bitsail-dist/target/bitsail-dist-${rversion}-bin</code>.</p><p><img src="'+l+'" alt="" loading="lazy"></p><h2 id="bitsail-product-structure" tabindex="-1"><a class="header-anchor" href="#bitsail-product-structure" aria-hidden="true">#</a> BitSail product structure</h2><p><img src="'+r+'" alt="" loading="lazy"></p><p><img src="'+t+`" alt="" loading="lazy"></p><h2 id="bitsail-job-submission" tabindex="-1"><a class="header-anchor" href="#bitsail-job-submission" aria-hidden="true">#</a> BitSail job submission</h2><h3 id="flink-session-job" tabindex="-1"><a class="header-anchor" href="#flink-session-job" aria-hidden="true">#</a> Flink Session Job</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>Step 1: Start the Flink Session cluster

Session operation requires the existence of hadoop dependencies in the local environment and the existence of the environment variable HADOOP_CLASSPATH.

bash ./embedded/flink/bin/start-cluster.sh

Step 2: Submit the job to the Flink Session cluster

bash bin/bitsail run \\
  --engine flink \\
  --execution-mode run \\
  --deployment-mode local \\
  --conf examples/Fake_Print_Example.json \\
  --jm-address &lt;job-manager-address&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="yarn-cluster-job" tabindex="-1"><a class="header-anchor" href="#yarn-cluster-job" aria-hidden="true">#</a> Yarn Cluster Job</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>Step 1: Set the HADOOP_HOME environment variable

export HADOOP_HOME=XXX

Step 2: Set HADOOP_HOME so that the submission client can find the configuration path of the yarn cluster, and then submit the job to the yarn cluster

bash ./bin/bitsail run --engine flink \\
--conf ~/dts_example/examples/Hive_Print_Example.json \\
--execution-mode run \\
--deployment-mode yarn-per-job \\
--queue default
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="bitsail-demo" tabindex="-1"><a class="header-anchor" href="#bitsail-demo" aria-hidden="true">#</a> BitSail Demo</h2><h3 id="fake-mysql" tabindex="-1"><a class="header-anchor" href="#fake-mysql" aria-hidden="true">#</a> Fake-&gt;MySQL</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>// create mysql table
CREATE TABLE \`bitsail_fake_source\` (
  \`id\` bigint(20) NOT NULL AUTO_INCREMENT,
  \`name\` varchar(255) DEFAULT NULL,
  \`price\` double DEFAULT NULL,
  \`image\` blob,
  \`start_time\` datetime DEFAULT NULL,
  \`end_time\` datetime DEFAULT NULL,
  \`order_id\` bigint(20) DEFAULT NULL,
  \`enabled\` tinyint(4) DEFAULT NULL,
  \`datetime\` int(11) DEFAULT NULL,
  PRIMARY KEY (\`id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="mysql-hive" tabindex="-1"><a class="header-anchor" href="#mysql-hive" aria-hidden="true">#</a> MySQL-&gt;Hive</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>// create hive table
CREATE TABLE \`bitsail\`.\`bitsail_mysql_hive\`(
  \`id\` bigint ,
  \`name\` string ,
  \`price\` double ,
  \`image\` binary,
  \`start_time\` timestamp ,
  \`end_time\` timestamp,
  \`order_id\` bigint ,
  \`enabled\` int,
  \`datetime\` int
)PARTITIONED BY (\`date\` string)
ROW FORMAT SERDE
  &#39;org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe&#39;
STORED AS INPUTFORMAT
  &#39;org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat&#39;
OUTPUTFORMAT
  &#39;org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat&#39;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,16);function S(E,L){const s=a("RouterLink"),d=a("ExternalLinkIcon");return c(),u("div",null,[h,e("p",null,[i("English | "),n(s,{to:"/zh/documents/start/quick_guide.html"},{default:v(()=>[i("简体中文")]),_:1})]),p,_,e("p",null,[e("a",g,[i("BitSail demo video"),n(d)])]),f])}const y=o(b,[["render",S],["__file","quick_guide.html.vue"]]);export{y as default};
