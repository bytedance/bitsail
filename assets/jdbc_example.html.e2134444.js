import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as p,c as o,a as n,b as s,d as e,w as u,e as c,r as l}from"./app.982a74e2.js";const i={},r=n("h1",{id:"jdbc-connector-example",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#jdbc-connector-example","aria-hidden":"true"},"#"),s(" JDBC connector example")],-1),d=c(`<h2 id="mysql-example" tabindex="-1"><a class="header-anchor" href="#mysql-example" aria-hidden="true">#</a> MySQL Example</h2><h3 id="mysql-source" tabindex="-1"><a class="header-anchor" href="#mysql-source" aria-hidden="true">#</a> MySQL source</h3><h4 id="mysql-table-sync" tabindex="-1"><a class="header-anchor" href="#mysql-table-sync" aria-hidden="true">#</a> MySQL table sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;bigint&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;split_pk&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;slaves&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                        <span class="token punctuation">{</span>
                            <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                        <span class="token punctuation">}</span>
                    <span class="token punctuation">]</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="mysql-sql-sync" tabindex="-1"><a class="header-anchor" href="#mysql-sql-sync" aria-hidden="true">#</a> MySQL SQL sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;index&quot;</span><span class="token operator">:</span><span class="token number">0</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;bigint&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;customized_sql&quot;</span><span class="token operator">:</span><span class="token string">&quot;select id, name, int_info, double_info, bytes_info from table_name where id &lt; 100&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;slaves&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                        <span class="token punctuation">{</span>
                            <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                        <span class="token punctuation">}</span>
                    <span class="token punctuation">]</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="mysql-sink" tabindex="-1"><a class="header-anchor" href="#mysql-sink" aria-hidden="true">#</a> MySQL sink</h3><h4 id="mysql-insert-sync" tabindex="-1"><a class="header-anchor" href="#mysql-insert-sync" aria-hidden="true">#</a> MySQL insert sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_value&quot;</span><span class="token operator">:</span><span class="token string">&quot;20221001&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_pattern_format&quot;</span><span class="token operator">:</span><span class="token string">&quot;yyyyMMdd&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;datetime&quot;</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="mysql-overwrite-sync" tabindex="-1"><a class="header-anchor" href="#mysql-overwrite-sync" aria-hidden="true">#</a> MySQL overwrite sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;write_mode&quot;</span><span class="token operator">:</span><span class="token string">&quot;overwrite&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="oracle-example" tabindex="-1"><a class="header-anchor" href="#oracle-example" aria-hidden="true">#</a> Oracle Example</h2><h3 id="oracle-source" tabindex="-1"><a class="header-anchor" href="#oracle-source" aria-hidden="true">#</a> Oracle source</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;ID&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;INT_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;integer&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;BIGINT_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;number&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">3</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;FLOAT_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;float&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">4</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;DOUBLE_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">5</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;RAW_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;raw&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                  <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">6</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;DATE_TYPE&quot;</span><span class="token punctuation">,</span>
                  <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_schema&quot;</span><span class="token operator">:</span><span class="token string">&quot;your schema name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;split_pk&quot;</span><span class="token operator">:</span><span class="token string">&quot;ID&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;slaves&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                        <span class="token punctuation">{</span>
							<span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:oracle:thin:@localhost:51912/TEST&quot;</span><span class="token punctuation">,</span>
							<span class="token property">&quot;host&quot;</span><span class="token operator">:</span><span class="token string">&quot;localhost&quot;</span><span class="token punctuation">,</span>
							<span class="token property">&quot;port&quot;</span><span class="token operator">:</span><span class="token number">51912</span>
						<span class="token punctuation">}</span>
                    <span class="token punctuation">]</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="oracle-sink" tabindex="-1"><a class="header-anchor" href="#oracle-sink" aria-hidden="true">#</a> Oracle sink</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
          <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.OracleOutputFormat&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;your table name&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;primary_key&quot;</span><span class="token operator">:</span> <span class="token string">&quot;ID&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
          <span class="token punctuation">]</span><span class="token punctuation">,</span>
          <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;password&quot;</span><span class="token operator">:</span> <span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;write_mode&quot;</span><span class="token operator">:</span> <span class="token string">&quot;insert&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;writer_parallelism_num&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
          <span class="token property">&quot;partition_name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;DATETIME&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;partition_value&quot;</span><span class="token operator">:</span> <span class="token string">&quot;20220705&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;partition_pattern_format&quot;</span><span class="token operator">:</span> <span class="token string">&quot;yyyyMMdd&quot;</span><span class="token punctuation">,</span>
          <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;ID&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;varchar&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;INT_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;integer&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;BIGINT_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;number&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">3</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;FLOAT_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;float&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">4</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;DOUBLE_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">5</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;RAW_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;raw&quot;</span>
            <span class="token punctuation">}</span><span class="token punctuation">,</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">6</span><span class="token punctuation">,</span>
              <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;DATE_TYPE&quot;</span><span class="token punctuation">,</span>
              <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date&quot;</span>
            <span class="token punctuation">}</span>
          <span class="token punctuation">]</span><span class="token punctuation">,</span>
          <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
            <span class="token punctuation">{</span>
              <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:oracle:thin:@localhost:1521/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;</span>
            <span class="token punctuation">}</span>
          <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="postgresql-example" tabindex="-1"><a class="header-anchor" href="#postgresql-example" aria-hidden="true">#</a> PostgreSQL Example</h2><h3 id="postgresql-source" tabindex="-1"><a class="header-anchor" href="#postgresql-source" aria-hidden="true">#</a> PostgreSQL source</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlInputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;index&quot;</span><span class="token operator">:</span><span class="token number">0</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;bigint&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytea&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_schema&quot;</span><span class="token operator">:</span><span class="token string">&quot;your schema name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;split_pk&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;slaves&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                        <span class="token punctuation">{</span>
                            <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;</span>
                        <span class="token punctuation">}</span>
                    <span class="token punctuation">]</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="postgresql-sink" tabindex="-1"><a class="header-anchor" href="#postgresql-sink" aria-hidden="true">#</a> PostgreSQL sink</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.PostgresqlOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_value&quot;</span><span class="token operator">:</span><span class="token string">&quot;20221001&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_pattern_format&quot;</span><span class="token operator">:</span><span class="token string">&quot;yyyyMMdd&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_schema&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table schema&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;primary_key&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;datetime&quot;</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="sqlserver-example" tabindex="-1"><a class="header-anchor" href="#sqlserver-example" aria-hidden="true">#</a> SqlServer Example</h2><h3 id="sqlserver-source" tabindex="-1"><a class="header-anchor" href="#sqlserver-source" aria-hidden="true">#</a> SqlServer source</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;reader&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerInputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;index&quot;</span><span class="token operator">:</span><span class="token number">0</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;bigint&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;your password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your db name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_schema&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table schema&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;your user name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;split_pk&quot;</span><span class="token operator">:</span><span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;slaves&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                        <span class="token punctuation">{</span>
                            <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test&quot;</span>
                        <span class="token punctuation">}</span>
                    <span class="token punctuation">]</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="sqlserver-sink" tabindex="-1"><a class="header-anchor" href="#sqlserver-sink" aria-hidden="true">#</a> SqlServer sink</h3><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.SqlServerOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_value&quot;</span><span class="token operator">:</span><span class="token string">&quot;20221001&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_pattern_format&quot;</span><span class="token operator">:</span><span class="token string">&quot;yyyyMMdd&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_schema&quot;</span><span class="token operator">:</span><span class="token string">&quot;your table schema&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;datetime&quot;</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,29);function k(v,q){const a=l("RouterLink");return p(),o("div",null,[r,n("p",null,[s("Parent document: "),e(a,{to:"/en/documents/connectors/jdbc/jdbc.html"},{default:u(()=>[s("JDBC connector")]),_:1})]),d])}const y=t(i,[["render",k],["__file","jdbc_example.html.vue"]]);export{y as default};
