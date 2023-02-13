import{_ as d,a as r,b as t}from"./product_structure.fcab2163.js";import{_ as c}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as v,c as u,a as e,d as n,w as o,b as i,e as b,r as s}from"./app.fe93ffff.js";const m={},h=e("h1",{id:"bitsail实机演示",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail实机演示","aria-hidden":"true"},"#"),i(" BitSail实机演示")],-1),_=e("hr",null,null,-1),p=e("h2",{id:"bitsail演示视频",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail演示视频","aria-hidden":"true"},"#"),i(" BitSail演示视频")],-1),g={href:"https://zhuanlan.zhihu.com/p/595157599",target:"_blank",rel:"noopener noreferrer"},f=e("h2",{id:"bitsail源码编译",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail源码编译","aria-hidden":"true"},"#"),i(" BitSail源码编译")],-1),S={href:"http://xn--BitSailbuild-cb5s42by2zvvwsh6dsj3cun6aipb46qfq5d726c.sh",target:"_blank",rel:"noopener noreferrer"},E=b('<p><img src="'+d+'" alt="" loading="lazy"></p><h2 id="bitsail产物结构" tabindex="-1"><a class="header-anchor" href="#bitsail产物结构" aria-hidden="true">#</a> BitSail产物结构</h2><p><img src="'+r+'" alt="" loading="lazy"></p><p><img src="'+t+`" alt="" loading="lazy"></p><h2 id="bitsail如何提交作业" tabindex="-1"><a class="header-anchor" href="#bitsail如何提交作业" aria-hidden="true">#</a> BitSail如何提交作业</h2><h3 id="flink-session-job" tabindex="-1"><a class="header-anchor" href="#flink-session-job" aria-hidden="true">#</a> Flink Session Job</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>第一步：启动Flink Session集群

session运行要求本地环境存在hadoop的依赖，同时需要HADOOP_CLASSPATH的环境变量存在。

bash ./embedded/flink/bin/start-cluster.sh

第二步：提交作业到Flink Session 集群

bash bin/bitsail run \\
  --engine flink \\
  --execution-mode run \\
  --deployment-mode local \\
  --conf examples/Fake_Print_Example.json \\
  --jm-address &lt;job-manager-address&gt;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="yarn-cluster-job" tabindex="-1"><a class="header-anchor" href="#yarn-cluster-job" aria-hidden="true">#</a> Yarn Cluster Job</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>第一步：设置HADOOP_HOME环境变量

export HADOOP_HOME=XXX

第二步：设置HADOOP_HOME，使提交客户端就找到yarn集群的配置路径，然后就可以提交作业到Yarn集群

bash ./bin/bitsail run --engine flink \\
--conf ~/dts_example/examples/Hive_Print_Example.json \\
--execution-mode run \\
--deployment-mode yarn-per-job \\
--queue default
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="bitsail-实机演示" tabindex="-1"><a class="header-anchor" href="#bitsail-实机演示" aria-hidden="true">#</a> BitSail 实机演示</h2><h3 id="fake-mysql" tabindex="-1"><a class="header-anchor" href="#fake-mysql" aria-hidden="true">#</a> Fake-&gt;MySQL</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>// 创建mysql表
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="mysql-hive" tabindex="-1"><a class="header-anchor" href="#mysql-hive" aria-hidden="true">#</a> MySQL-&gt;Hive</h3><div class="language-Shell line-numbers-mode" data-ext="Shell"><pre class="language-Shell"><code>// 创建hive表
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,14);function L(x,T){const l=s("RouterLink"),a=s("ExternalLinkIcon");return v(),u("div",null,[h,e("p",null,[n(l,{to:"/en/documents/start/quick_guide.html"},{default:o(()=>[i("English")]),_:1}),i(" | 简体中文")]),_,p,e("p",null,[e("a",g,[i("BitSail实机演示"),n(a)])]),f,e("p",null,[e("a",S,[i("BitSail在项目中内置了编译脚本build.sh"),n(a)]),i("，存放在项目根目录中。新下载的用户可以直接该脚本进行编译，编译成功后可以在目录：bitsail-dist/target/bitsail-dist-${rversion}-bin 中找到相应的产物。")]),E])}const O=c(m,[["render",L],["__file","quick_guide.html.vue"]]);export{O as default};
