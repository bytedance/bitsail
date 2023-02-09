function Y(){return{baseUrl:null,breaks:!1,extensions:null,gfm:!0,headerIds:!0,headerPrefix:"",highlight:null,langPrefix:"language-",mangle:!0,pedantic:!1,renderer:null,sanitize:!1,sanitizer:null,silent:!1,smartLists:!1,smartypants:!1,tokenizer:null,walkTokens:null,xhtml:!1}}let E={baseUrl:null,breaks:!1,extensions:null,gfm:!0,headerIds:!0,headerPrefix:"",highlight:null,langPrefix:"language-",mangle:!0,pedantic:!1,renderer:null,sanitize:!1,sanitizer:null,silent:!1,smartLists:!1,smartypants:!1,tokenizer:null,walkTokens:null,xhtml:!1};const F=/[&<>"']/,X=/[&<>"']/g,G=/[<>"']|&(?!#?\w+;)/,tt=/[<>"']|&(?!#?\w+;)/g,et={"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"},D=o=>et[o];function x(o,e){if(e){if(F.test(o))return o.replace(X,D)}else if(G.test(o))return o.replace(tt,D);return o}const nt=/&(#(?:\d+)|(?:#x[0-9A-Fa-f]+)|(?:\w+));?/gi;function J(o){return o.replace(nt,(e,t)=>(t=t.toLowerCase())==="colon"?":":t.charAt(0)==="#"?t.charAt(1)==="x"?String.fromCharCode(parseInt(t.substring(2),16)):String.fromCharCode(+t.substring(1)):"")}const it=/(^|[^\[])\^/g;function k(o,e){o=o.source||o,e=e||"";const t={replace:(n,i)=>(i=(i=i.source||i).replace(it,"$1"),o=o.replace(n,i),t),getRegex:()=>new RegExp(o,e)};return t}const st=/[^\w:]/g,rt=/^$|^[a-z][a-z0-9+.-]*:|^[?#]/i;function U(o,e,t){if(o){let n;try{n=decodeURIComponent(J(t)).replace(st,"").toLowerCase()}catch{return null}if(n.indexOf("javascript:")===0||n.indexOf("vbscript:")===0||n.indexOf("data:")===0)return null}e&&!rt.test(t)&&(t=function(n,i){M[" "+n]||(at.test(n)?M[" "+n]=n+"/":M[" "+n]=q(n,"/",!0));const s=(n=M[" "+n]).indexOf(":")===-1;return i.substring(0,2)==="//"?s?i:n.replace(lt,"$1")+i:i.charAt(0)==="/"?s?i:n.replace(ot,"$1")+i:n+i}(e,t));try{t=encodeURI(t).replace(/%25/g,"%")}catch{return null}return t}const M={},at=/^[^:]+:\/*[^/]*$/,lt=/^([^:]+:)[\s\S]*$/,ot=/^([^:]+:\/*[^/]*)[\s\S]*$/,O={exec:function(){}};function v(o){let e,t,n=1;for(;n<arguments.length;n++)for(t in e=arguments[n],e)Object.prototype.hasOwnProperty.call(e,t)&&(o[t]=e[t]);return o}function Z(o,e){const t=o.replace(/\|/g,(i,s,r)=>{let l=!1,a=s;for(;--a>=0&&r[a]==="\\";)l=!l;return l?"|":" |"}).split(/ \|/);let n=0;if(t[0].trim()||t.shift(),t.length>0&&!t[t.length-1].trim()&&t.pop(),t.length>e)t.splice(e);else for(;t.length<e;)t.push("");for(;n<t.length;n++)t[n]=t[n].trim().replace(/\\\|/g,"|");return t}function q(o,e,t){const n=o.length;if(n===0)return"";let i=0;for(;i<n;){const s=o.charAt(n-i-1);if(s!==e||t){if(s===e||!t)break;i++}else i++}return o.substr(0,n-i)}function Q(o){o&&o.sanitize&&!o.silent&&console.warn("marked(): sanitize and sanitizer parameters are deprecated since version 0.7.0, should not be used and will be removed in the future. Read more here: https://marked.js.org/#/USING_ADVANCED.md#options")}function j(o,e){if(e<1)return"";let t="";for(;e>1;)1&e&&(t+=o),e>>=1,o+=o;return t+o}function B(o,e,t,n){const i=e.href,s=e.title?x(e.title):null,r=o[1].replace(/\\([\[\]])/g,"$1");if(o[0].charAt(0)!=="!"){n.state.inLink=!0;const l={type:"link",raw:t,href:i,title:s,text:r,tokens:n.inlineTokens(r,[])};return n.state.inLink=!1,l}return{type:"image",raw:t,href:i,title:s,text:x(r)}}class P{constructor(e){this.options=e||E}space(e){const t=this.rules.block.newline.exec(e);if(t&&t[0].length>0)return{type:"space",raw:t[0]}}code(e){const t=this.rules.block.code.exec(e);if(t){const n=t[0].replace(/^ {1,4}/gm,"");return{type:"code",raw:t[0],codeBlockStyle:"indented",text:this.options.pedantic?n:q(n,`
`)}}}fences(e){const t=this.rules.block.fences.exec(e);if(t){const n=t[0],i=function(s,r){const l=s.match(/^(\s+)(?:```)/);if(l===null)return r;const a=l[1];return r.split(`
`).map(c=>{const u=c.match(/^\s+/);if(u===null)return c;const[g]=u;return g.length>=a.length?c.slice(a.length):c}).join(`
`)}(n,t[3]||"");return{type:"code",raw:n,lang:t[2]?t[2].trim():t[2],text:i}}}heading(e){const t=this.rules.block.heading.exec(e);if(t){let n=t[2].trim();if(/#$/.test(n)){const s=q(n,"#");this.options.pedantic?n=s.trim():s&&!/ $/.test(s)||(n=s.trim())}const i={type:"heading",raw:t[0],depth:t[1].length,text:n,tokens:[]};return this.lexer.inline(i.text,i.tokens),i}}hr(e){const t=this.rules.block.hr.exec(e);if(t)return{type:"hr",raw:t[0]}}blockquote(e){const t=this.rules.block.blockquote.exec(e);if(t){const n=t[0].replace(/^ *> ?/gm,"");return{type:"blockquote",raw:t[0],tokens:this.lexer.blockTokens(n,[]),text:n}}}list(e){let t=this.rules.block.list.exec(e);if(t){let n,i,s,r,l,a,c,u,g,d,w,A,y=t[1].trim();const T=y.length>1,f={type:"list",raw:"",ordered:T,start:T?+y.slice(0,-1):"",loose:!1,items:[]};y=T?`\\d{1,9}\\${y.slice(-1)}`:`\\${y}`,this.options.pedantic&&(y=T?y:"[*+-]");const $=new RegExp(`^( {0,3}${y})((?: [^\\n]*)?(?:\\n|$))`);for(;e&&(A=!1,t=$.exec(e))&&!this.rules.block.hr.test(e);){if(n=t[0],e=e.substring(n.length),u=t[2].split(`
`,1)[0],g=e.split(`
`,1)[0],this.options.pedantic?(r=2,w=u.trimLeft()):(r=t[2].search(/[^ ]/),r=r>4?1:r,w=u.slice(r),r+=t[1].length),a=!1,!u&&/^ *$/.test(g)&&(n+=g+`
`,e=e.substring(g.length+1),A=!0),!A){const S=new RegExp(`^ {0,${Math.min(3,r-1)}}(?:[*+-]|\\d{1,9}[.)])`);for(;e&&(d=e.split(`
`,1)[0],u=d,this.options.pedantic&&(u=u.replace(/^ {1,4}(?=( {4})*[^ ])/g,"  ")),!S.test(u));){if(u.search(/[^ ]/)>=r||!u.trim())w+=`
`+u.slice(r);else{if(a)break;w+=`
`+u}a||u.trim()||(a=!0),n+=d+`
`,e=e.substring(d.length+1)}}f.loose||(c?f.loose=!0:/\n *\n *$/.test(n)&&(c=!0)),this.options.gfm&&(i=/^\[[ xX]\] /.exec(w),i&&(s=i[0]!=="[ ] ",w=w.replace(/^\[[ xX]\] +/,""))),f.items.push({type:"list_item",raw:n,task:!!i,checked:s,loose:!1,text:w}),f.raw+=n}f.items[f.items.length-1].raw=n.trimRight(),f.items[f.items.length-1].text=w.trimRight(),f.raw=f.raw.trimRight();const I=f.items.length;for(l=0;l<I;l++){this.lexer.state.top=!1,f.items[l].tokens=this.lexer.blockTokens(f.items[l].text,[]);const S=f.items[l].tokens.filter(b=>b.type==="space"),L=S.every(b=>{const C=b.raw.split("");let R=0;for(const V of C)if(V===`
`&&(R+=1),R>1)return!0;return!1});!f.loose&&S.length&&L&&(f.loose=!0,f.items[l].loose=!0)}return f}}html(e){const t=this.rules.block.html.exec(e);if(t){const n={type:"html",raw:t[0],pre:!this.options.sanitizer&&(t[1]==="pre"||t[1]==="script"||t[1]==="style"),text:t[0]};return this.options.sanitize&&(n.type="paragraph",n.text=this.options.sanitizer?this.options.sanitizer(t[0]):x(t[0]),n.tokens=[],this.lexer.inline(n.text,n.tokens)),n}}def(e){const t=this.rules.block.def.exec(e);if(t)return t[3]&&(t[3]=t[3].substring(1,t[3].length-1)),{type:"def",tag:t[1].toLowerCase().replace(/\s+/g," "),raw:t[0],href:t[2],title:t[3]}}table(e){const t=this.rules.block.table.exec(e);if(t){const n={type:"table",header:Z(t[1]).map(i=>({text:i})),align:t[2].replace(/^ *|\| *$/g,"").split(/ *\| */),rows:t[3]&&t[3].trim()?t[3].replace(/\n[ \t]*$/,"").split(`
`):[]};if(n.header.length===n.align.length){n.raw=t[0];let i,s,r,l,a=n.align.length;for(i=0;i<a;i++)/^ *-+: *$/.test(n.align[i])?n.align[i]="right":/^ *:-+: *$/.test(n.align[i])?n.align[i]="center":/^ *:-+ *$/.test(n.align[i])?n.align[i]="left":n.align[i]=null;for(a=n.rows.length,i=0;i<a;i++)n.rows[i]=Z(n.rows[i],n.header.length).map(c=>({text:c}));for(a=n.header.length,s=0;s<a;s++)n.header[s].tokens=[],this.lexer.inlineTokens(n.header[s].text,n.header[s].tokens);for(a=n.rows.length,s=0;s<a;s++)for(l=n.rows[s],r=0;r<l.length;r++)l[r].tokens=[],this.lexer.inlineTokens(l[r].text,l[r].tokens);return n}}}lheading(e){const t=this.rules.block.lheading.exec(e);if(t){const n={type:"heading",raw:t[0],depth:t[2].charAt(0)==="="?1:2,text:t[1],tokens:[]};return this.lexer.inline(n.text,n.tokens),n}}paragraph(e){const t=this.rules.block.paragraph.exec(e);if(t){const n={type:"paragraph",raw:t[0],text:t[1].charAt(t[1].length-1)===`
`?t[1].slice(0,-1):t[1],tokens:[]};return this.lexer.inline(n.text,n.tokens),n}}text(e){const t=this.rules.block.text.exec(e);if(t){const n={type:"text",raw:t[0],text:t[0],tokens:[]};return this.lexer.inline(n.text,n.tokens),n}}escape(e){const t=this.rules.inline.escape.exec(e);if(t)return{type:"escape",raw:t[0],text:x(t[1])}}tag(e){const t=this.rules.inline.tag.exec(e);if(t)return!this.lexer.state.inLink&&/^<a /i.test(t[0])?this.lexer.state.inLink=!0:this.lexer.state.inLink&&/^<\/a>/i.test(t[0])&&(this.lexer.state.inLink=!1),!this.lexer.state.inRawBlock&&/^<(pre|code|kbd|script)(\s|>)/i.test(t[0])?this.lexer.state.inRawBlock=!0:this.lexer.state.inRawBlock&&/^<\/(pre|code|kbd|script)(\s|>)/i.test(t[0])&&(this.lexer.state.inRawBlock=!1),{type:this.options.sanitize?"text":"html",raw:t[0],inLink:this.lexer.state.inLink,inRawBlock:this.lexer.state.inRawBlock,text:this.options.sanitize?this.options.sanitizer?this.options.sanitizer(t[0]):x(t[0]):t[0]}}link(e){const t=this.rules.inline.link.exec(e);if(t){const n=t[2].trim();if(!this.options.pedantic&&/^</.test(n)){if(!/>$/.test(n))return;const r=q(n.slice(0,-1),"\\");if((n.length-r.length)%2==0)return}else{const r=function(l,a){if(l.indexOf(a[1])===-1)return-1;const c=l.length;let u=0,g=0;for(;g<c;g++)if(l[g]==="\\")g++;else if(l[g]===a[0])u++;else if(l[g]===a[1]&&(u--,u<0))return g;return-1}(t[2],"()");if(r>-1){const l=(t[0].indexOf("!")===0?5:4)+t[1].length+r;t[2]=t[2].substring(0,r),t[0]=t[0].substring(0,l).trim(),t[3]=""}}let i=t[2],s="";if(this.options.pedantic){const r=/^([^'"]*[^\s])\s+(['"])(.*)\2/.exec(i);r&&(i=r[1],s=r[3])}else s=t[3]?t[3].slice(1,-1):"";return i=i.trim(),/^</.test(i)&&(i=this.options.pedantic&&!/>$/.test(n)?i.slice(1):i.slice(1,-1)),B(t,{href:i&&i.replace(this.rules.inline._escapes,"$1"),title:s&&s.replace(this.rules.inline._escapes,"$1")},t[0],this.lexer)}}reflink(e,t){let n;if((n=this.rules.inline.reflink.exec(e))||(n=this.rules.inline.nolink.exec(e))){let i=(n[2]||n[1]).replace(/\s+/g," ");if(i=t[i.toLowerCase()],!i||!i.href){const s=n[0].charAt(0);return{type:"text",raw:s,text:s}}return B(n,i,n[0],this.lexer)}}emStrong(e,t,n=""){let i=this.rules.inline.emStrong.lDelim.exec(e);if(!i||i[3]&&n.match(/[\p{L}\p{N}]/u))return;const s=i[1]||i[2]||"";if(!s||s&&(n===""||this.rules.inline.punctuation.exec(n))){const r=i[0].length-1;let l,a,c=r,u=0;const g=i[0][0]==="*"?this.rules.inline.emStrong.rDelimAst:this.rules.inline.emStrong.rDelimUnd;for(g.lastIndex=0,t=t.slice(-1*e.length+r);(i=g.exec(t))!=null;){if(l=i[1]||i[2]||i[3]||i[4]||i[5]||i[6],!l)continue;if(a=l.length,i[3]||i[4]){c+=a;continue}if((i[5]||i[6])&&r%3&&!((r+a)%3)){u+=a;continue}if(c-=a,c>0)continue;if(a=Math.min(a,a+c+u),Math.min(r,a)%2){const w=e.slice(1,r+i.index+a);return{type:"em",raw:e.slice(0,r+i.index+a+1),text:w,tokens:this.lexer.inlineTokens(w,[])}}const d=e.slice(2,r+i.index+a-1);return{type:"strong",raw:e.slice(0,r+i.index+a+1),text:d,tokens:this.lexer.inlineTokens(d,[])}}}}codespan(e){const t=this.rules.inline.code.exec(e);if(t){let n=t[2].replace(/\n/g," ");const i=/[^ ]/.test(n),s=/^ /.test(n)&&/ $/.test(n);return i&&s&&(n=n.substring(1,n.length-1)),n=x(n,!0),{type:"codespan",raw:t[0],text:n}}}br(e){const t=this.rules.inline.br.exec(e);if(t)return{type:"br",raw:t[0]}}del(e){const t=this.rules.inline.del.exec(e);if(t)return{type:"del",raw:t[0],text:t[2],tokens:this.lexer.inlineTokens(t[2],[])}}autolink(e,t){const n=this.rules.inline.autolink.exec(e);if(n){let i,s;return n[2]==="@"?(i=x(this.options.mangle?t(n[1]):n[1]),s="mailto:"+i):(i=x(n[1]),s=i),{type:"link",raw:n[0],text:i,href:s,tokens:[{type:"text",raw:i,text:i}]}}}url(e,t){let n;if(n=this.rules.inline.url.exec(e)){let i,s;if(n[2]==="@")i=x(this.options.mangle?t(n[0]):n[0]),s="mailto:"+i;else{let r;do r=n[0],n[0]=this.rules.inline._backpedal.exec(n[0])[0];while(r!==n[0]);i=x(n[0]),s=n[1]==="www."?"http://"+i:i}return{type:"link",raw:n[0],text:i,href:s,tokens:[{type:"text",raw:i,text:i}]}}}inlineText(e,t){const n=this.rules.inline.text.exec(e);if(n){let i;return i=this.lexer.state.inRawBlock?this.options.sanitize?this.options.sanitizer?this.options.sanitizer(n[0]):x(n[0]):n[0]:x(this.options.smartypants?t(n[0]):n[0]),{type:"text",raw:n[0],text:i}}}}const h={newline:/^(?: *(?:\n|$))+/,code:/^( {4}[^\n]+(?:\n(?: *(?:\n|$))*)?)+/,fences:/^ {0,3}(`{3,}(?=[^`\n]*\n)|~{3,})([^\n]*)\n(?:|([\s\S]*?)\n)(?: {0,3}\1[~`]* *(?=\n|$)|$)/,hr:/^ {0,3}((?:- *){3,}|(?:_ *){3,}|(?:\* *){3,})(?:\n+|$)/,heading:/^ {0,3}(#{1,6})(?=\s|$)(.*)(?:\n+|$)/,blockquote:/^( {0,3}> ?(paragraph|[^\n]*)(?:\n|$))+/,list:/^( {0,3}bull)( [^\n]+?)?(?:\n|$)/,html:"^ {0,3}(?:<(script|pre|style|textarea)[\\s>][\\s\\S]*?(?:</\\1>[^\\n]*\\n+|$)|comment[^\\n]*(\\n+|$)|<\\?[\\s\\S]*?(?:\\?>\\n*|$)|<![A-Z][\\s\\S]*?(?:>\\n*|$)|<!\\[CDATA\\[[\\s\\S]*?(?:\\]\\]>\\n*|$)|</?(tag)(?: +|\\n|/?>)[\\s\\S]*?(?:(?:\\n *)+\\n|$)|<(?!script|pre|style|textarea)([a-z][\\w-]*)(?:attribute)*? */?>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n *)+\\n|$)|</(?!script|pre|style|textarea)[a-z][\\w-]*\\s*>(?=[ \\t]*(?:\\n|$))[\\s\\S]*?(?:(?:\\n *)+\\n|$))",def:/^ {0,3}\[(label)\]: *(?:\n *)?<?([^\s>]+)>?(?:(?: +(?:\n *)?| *\n *)(title))? *(?:\n+|$)/,table:O,lheading:/^([^\n]+)\n {0,3}(=+|-+) *(?:\n+|$)/,_paragraph:/^([^\n]+(?:\n(?!hr|heading|lheading|blockquote|fences|list|html|table| +\n)[^\n]+)*)/,text:/^[^\n]+/,_label:/(?!\s*\])(?:\\.|[^\[\]\\])+/,_title:/(?:"(?:\\"?|[^"\\])*"|'[^'\n]*(?:\n[^'\n]+)*\n?'|\([^()]*\))/};h.def=k(h.def).replace("label",h._label).replace("title",h._title).getRegex(),h.bullet=/(?:[*+-]|\d{1,9}[.)])/,h.listItemStart=k(/^( *)(bull) */).replace("bull",h.bullet).getRegex(),h.list=k(h.list).replace(/bull/g,h.bullet).replace("hr","\\n+(?=\\1?(?:(?:- *){3,}|(?:_ *){3,}|(?:\\* *){3,})(?:\\n+|$))").replace("def","\\n+(?="+h.def.source+")").getRegex(),h._tag="address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|meta|nav|noframes|ol|optgroup|option|p|param|section|source|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul",h._comment=/<!--(?!-?>)[\s\S]*?(?:-->|$)/,h.html=k(h.html,"i").replace("comment",h._comment).replace("tag",h._tag).replace("attribute",/ +[a-zA-Z:_][\w.:-]*(?: *= *"[^"\n]*"| *= *'[^'\n]*'| *= *[^\s"'=<>`]+)?/).getRegex(),h.paragraph=k(h._paragraph).replace("hr",h.hr).replace("heading"," {0,3}#{1,6} ").replace("|lheading","").replace("|table","").replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)]) ").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",h._tag).getRegex(),h.blockquote=k(h.blockquote).replace("paragraph",h.paragraph).getRegex(),h.normal=v({},h),h.gfm=v({},h.normal,{table:"^ *([^\\n ].*\\|.*)\\n {0,3}(?:\\| *)?(:?-+:? *(?:\\| *:?-+:? *)*)(?:\\| *)?(?:\\n((?:(?! *\\n|hr|heading|blockquote|code|fences|list|html).*(?:\\n|$))*)\\n*|$)"}),h.gfm.table=k(h.gfm.table).replace("hr",h.hr).replace("heading"," {0,3}#{1,6} ").replace("blockquote"," {0,3}>").replace("code"," {4}[^\\n]").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)]) ").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",h._tag).getRegex(),h.gfm.paragraph=k(h._paragraph).replace("hr",h.hr).replace("heading"," {0,3}#{1,6} ").replace("|lheading","").replace("table",h.gfm.table).replace("blockquote"," {0,3}>").replace("fences"," {0,3}(?:`{3,}(?=[^`\\n]*\\n)|~{3,})[^\\n]*\\n").replace("list"," {0,3}(?:[*+-]|1[.)]) ").replace("html","</?(?:tag)(?: +|\\n|/?>)|<(?:script|pre|style|textarea|!--)").replace("tag",h._tag).getRegex(),h.pedantic=v({},h.normal,{html:k(`^ *(?:comment *(?:\\n|\\s*$)|<(tag)[\\s\\S]+?</\\1> *(?:\\n{2,}|\\s*$)|<tag(?:"[^"]*"|'[^']*'|\\s[^'"/>\\s]*)*?/?> *(?:\\n{2,}|\\s*$))`).replace("comment",h._comment).replace(/tag/g,"(?!(?:a|em|strong|small|s|cite|q|dfn|abbr|data|time|code|var|samp|kbd|sub|sup|i|b|u|mark|ruby|rt|rp|bdi|bdo|span|br|wbr|ins|del|img)\\b)\\w+(?!:|[^\\w\\s@]*@)\\b").getRegex(),def:/^ *\[([^\]]+)\]: *<?([^\s>]+)>?(?: +(["(][^\n]+[")]))? *(?:\n+|$)/,heading:/^(#{1,6})(.*)(?:\n+|$)/,fences:O,paragraph:k(h.normal._paragraph).replace("hr",h.hr).replace("heading",` *#{1,6} *[^
]`).replace("lheading",h.lheading).replace("blockquote"," {0,3}>").replace("|fences","").replace("|list","").replace("|html","").getRegex()});const p={escape:/^\\([!"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~])/,autolink:/^<(scheme:[^\s\x00-\x1f<>]*|email)>/,url:O,tag:"^comment|^</[a-zA-Z][\\w:-]*\\s*>|^<[a-zA-Z][\\w-]*(?:attribute)*?\\s*/?>|^<\\?[\\s\\S]*?\\?>|^<![a-zA-Z]+\\s[\\s\\S]*?>|^<!\\[CDATA\\[[\\s\\S]*?\\]\\]>",link:/^!?\[(label)\]\(\s*(href)(?:\s+(title))?\s*\)/,reflink:/^!?\[(label)\]\[(ref)\]/,nolink:/^!?\[(ref)\](?:\[\])?/,reflinkSearch:"reflink|nolink(?!\\()",emStrong:{lDelim:/^(?:\*+(?:([punct_])|[^\s*]))|^_+(?:([punct*])|([^\s_]))/,rDelimAst:/^[^_*]*?\_\_[^_*]*?\*[^_*]*?(?=\_\_)|[punct_](\*+)(?=[\s]|$)|[^punct*_\s](\*+)(?=[punct_\s]|$)|[punct_\s](\*+)(?=[^punct*_\s])|[\s](\*+)(?=[punct_])|[punct_](\*+)(?=[punct_])|[^punct*_\s](\*+)(?=[^punct*_\s])/,rDelimUnd:/^[^_*]*?\*\*[^_*]*?\_[^_*]*?(?=\*\*)|[punct*](\_+)(?=[\s]|$)|[^punct*_\s](\_+)(?=[punct*\s]|$)|[punct*\s](\_+)(?=[^punct*_\s])|[\s](\_+)(?=[punct*])|[punct*](\_+)(?=[punct*])/},code:/^(`+)([^`]|[^`][\s\S]*?[^`])\1(?!`)/,br:/^( {2,}|\\)\n(?!\s*$)/,del:O,text:/^(`+|[^`])(?:(?= {2,}\n)|[\s\S]*?(?:(?=[\\<!\[`*_]|\b_|$)|[^ ](?= {2,}\n)))/,punctuation:/^([\spunctuation])/};function ct(o){return o.replace(/---/g,"—").replace(/--/g,"–").replace(/(^|[-\u2014/(\[{"\s])'/g,"$1‘").replace(/'/g,"’").replace(/(^|[-\u2014/(\[{\u2018\s])"/g,"$1“").replace(/"/g,"”").replace(/\.{3}/g,"…")}function H(o){let e,t,n="";const i=o.length;for(e=0;e<i;e++)t=o.charCodeAt(e),Math.random()>.5&&(t="x"+t.toString(16)),n+="&#"+t+";";return n}p._punctuation="!\"#$%&'()+\\-.,/:;<=>?@\\[\\]`^{|}~",p.punctuation=k(p.punctuation).replace(/punctuation/g,p._punctuation).getRegex(),p.blockSkip=/\[[^\]]*?\]\([^\)]*?\)|`[^`]*?`|<[^>]*?>/g,p.escapedEmSt=/\\\*|\\_/g,p._comment=k(h._comment).replace("(?:-->|$)","-->").getRegex(),p.emStrong.lDelim=k(p.emStrong.lDelim).replace(/punct/g,p._punctuation).getRegex(),p.emStrong.rDelimAst=k(p.emStrong.rDelimAst,"g").replace(/punct/g,p._punctuation).getRegex(),p.emStrong.rDelimUnd=k(p.emStrong.rDelimUnd,"g").replace(/punct/g,p._punctuation).getRegex(),p._escapes=/\\([!"#$%&'()*+,\-./:;<=>?@\[\]\\^_`{|}~])/g,p._scheme=/[a-zA-Z][a-zA-Z0-9+.-]{1,31}/,p._email=/[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+(@)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(?![-_])/,p.autolink=k(p.autolink).replace("scheme",p._scheme).replace("email",p._email).getRegex(),p._attribute=/\s+[a-zA-Z:_][\w.:-]*(?:\s*=\s*"[^"]*"|\s*=\s*'[^']*'|\s*=\s*[^\s"'=<>`]+)?/,p.tag=k(p.tag).replace("comment",p._comment).replace("attribute",p._attribute).getRegex(),p._label=/(?:\[(?:\\.|[^\[\]\\])*\]|\\.|`[^`]*`|[^\[\]\\`])*?/,p._href=/<(?:\\.|[^\n<>\\])+>|[^\s\x00-\x1f]*/,p._title=/"(?:\\"?|[^"\\])*"|'(?:\\'?|[^'\\])*'|\((?:\\\)?|[^)\\])*\)/,p.link=k(p.link).replace("label",p._label).replace("href",p._href).replace("title",p._title).getRegex(),p.reflink=k(p.reflink).replace("label",p._label).replace("ref",h._label).getRegex(),p.nolink=k(p.nolink).replace("ref",h._label).getRegex(),p.reflinkSearch=k(p.reflinkSearch,"g").replace("reflink",p.reflink).replace("nolink",p.nolink).getRegex(),p.normal=v({},p),p.pedantic=v({},p.normal,{strong:{start:/^__|\*\*/,middle:/^__(?=\S)([\s\S]*?\S)__(?!_)|^\*\*(?=\S)([\s\S]*?\S)\*\*(?!\*)/,endAst:/\*\*(?!\*)/g,endUnd:/__(?!_)/g},em:{start:/^_|\*/,middle:/^()\*(?=\S)([\s\S]*?\S)\*(?!\*)|^_(?=\S)([\s\S]*?\S)_(?!_)/,endAst:/\*(?!\*)/g,endUnd:/_(?!_)/g},link:k(/^!?\[(label)\]\((.*?)\)/).replace("label",p._label).getRegex(),reflink:k(/^!?\[(label)\]\s*\[([^\]]*)\]/).replace("label",p._label).getRegex()}),p.gfm=v({},p.normal,{escape:k(p.escape).replace("])","~|])").getRegex(),_extended_email:/[A-Za-z0-9._+-]+(@)[a-zA-Z0-9-_]+(?:\.[a-zA-Z0-9-_]*[a-zA-Z0-9])+(?![-_])/,url:/^((?:ftp|https?):\/\/|www\.)(?:[a-zA-Z0-9\-]+\.?)+[^\s<]*|^email/,_backpedal:/(?:[^?!.,:;*_~()&]+|\([^)]*\)|&(?![a-zA-Z0-9]+;$)|[?!.,:;*_~)]+(?!$))+/,del:/^(~~?)(?=[^\s~])([\s\S]*?[^\s~])\1(?=[^~]|$)/,text:/^([`~]+|[^`~])(?:(?= {2,}\n)|(?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)|[\s\S]*?(?:(?=[\\<!\[`*~_]|\b_|https?:\/\/|ftp:\/\/|www\.|$)|[^ ](?= {2,}\n)|[^a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-](?=[a-zA-Z0-9.!#$%&'*+\/=?_`{\|}~-]+@)))/}),p.gfm.url=k(p.gfm.url,"i").replace("email",p.gfm._extended_email).getRegex(),p.breaks=v({},p.gfm,{br:k(p.br).replace("{2,}","*").getRegex(),text:k(p.gfm.text).replace("\\b_","\\b_| {2,}\\n").replace(/\{2,\}/g,"*").getRegex()});class _{constructor(e){this.tokens=[],this.tokens.links=Object.create(null),this.options=e||E,this.options.tokenizer=this.options.tokenizer||new P,this.tokenizer=this.options.tokenizer,this.tokenizer.options=this.options,this.tokenizer.lexer=this,this.inlineQueue=[],this.state={inLink:!1,inRawBlock:!1,top:!0};const t={block:h.normal,inline:p.normal};this.options.pedantic?(t.block=h.pedantic,t.inline=p.pedantic):this.options.gfm&&(t.block=h.gfm,this.options.breaks?t.inline=p.breaks:t.inline=p.gfm),this.tokenizer.rules=t}static get rules(){return{block:h,inline:p}}static lex(e,t){return new _(t).lex(e)}static lexInline(e,t){return new _(t).inlineTokens(e)}lex(e){let t;for(e=e.replace(/\r\n|\r/g,`
`).replace(/\t/g,"    "),this.blockTokens(e,this.tokens);t=this.inlineQueue.shift();)this.inlineTokens(t.src,t.tokens);return this.tokens}blockTokens(e,t=[]){let n,i,s,r;for(this.options.pedantic&&(e=e.replace(/^ +$/gm,""));e;)if(!(this.options.extensions&&this.options.extensions.block&&this.options.extensions.block.some(l=>!!(n=l.call({lexer:this},e,t))&&(e=e.substring(n.raw.length),t.push(n),!0))))if(n=this.tokenizer.space(e))e=e.substring(n.raw.length),n.raw.length===1&&t.length>0?t[t.length-1].raw+=`
`:t.push(n);else if(n=this.tokenizer.code(e))e=e.substring(n.raw.length),i=t[t.length-1],!i||i.type!=="paragraph"&&i.type!=="text"?t.push(n):(i.raw+=`
`+n.raw,i.text+=`
`+n.text,this.inlineQueue[this.inlineQueue.length-1].src=i.text);else if(n=this.tokenizer.fences(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.heading(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.hr(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.blockquote(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.list(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.html(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.def(e))e=e.substring(n.raw.length),i=t[t.length-1],!i||i.type!=="paragraph"&&i.type!=="text"?this.tokens.links[n.tag]||(this.tokens.links[n.tag]={href:n.href,title:n.title}):(i.raw+=`
`+n.raw,i.text+=`
`+n.raw,this.inlineQueue[this.inlineQueue.length-1].src=i.text);else if(n=this.tokenizer.table(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.lheading(e))e=e.substring(n.raw.length),t.push(n);else{if(s=e,this.options.extensions&&this.options.extensions.startBlock){let l=1/0;const a=e.slice(1);let c;this.options.extensions.startBlock.forEach(function(u){c=u.call({lexer:this},a),typeof c=="number"&&c>=0&&(l=Math.min(l,c))}),l<1/0&&l>=0&&(s=e.substring(0,l+1))}if(this.state.top&&(n=this.tokenizer.paragraph(s)))i=t[t.length-1],r&&i.type==="paragraph"?(i.raw+=`
`+n.raw,i.text+=`
`+n.text,this.inlineQueue.pop(),this.inlineQueue[this.inlineQueue.length-1].src=i.text):t.push(n),r=s.length!==e.length,e=e.substring(n.raw.length);else if(n=this.tokenizer.text(e))e=e.substring(n.raw.length),i=t[t.length-1],i&&i.type==="text"?(i.raw+=`
`+n.raw,i.text+=`
`+n.text,this.inlineQueue.pop(),this.inlineQueue[this.inlineQueue.length-1].src=i.text):t.push(n);else if(e){const l="Infinite loop on byte: "+e.charCodeAt(0);if(this.options.silent){console.error(l);break}throw new Error(l)}}return this.state.top=!0,t}inline(e,t){this.inlineQueue.push({src:e,tokens:t})}inlineTokens(e,t=[]){let n,i,s,r,l,a,c=e;if(this.tokens.links){const u=Object.keys(this.tokens.links);if(u.length>0)for(;(r=this.tokenizer.rules.inline.reflinkSearch.exec(c))!=null;)u.includes(r[0].slice(r[0].lastIndexOf("[")+1,-1))&&(c=c.slice(0,r.index)+"["+j("a",r[0].length-2)+"]"+c.slice(this.tokenizer.rules.inline.reflinkSearch.lastIndex))}for(;(r=this.tokenizer.rules.inline.blockSkip.exec(c))!=null;)c=c.slice(0,r.index)+"["+j("a",r[0].length-2)+"]"+c.slice(this.tokenizer.rules.inline.blockSkip.lastIndex);for(;(r=this.tokenizer.rules.inline.escapedEmSt.exec(c))!=null;)c=c.slice(0,r.index)+"++"+c.slice(this.tokenizer.rules.inline.escapedEmSt.lastIndex);for(;e;)if(l||(a=""),l=!1,!(this.options.extensions&&this.options.extensions.inline&&this.options.extensions.inline.some(u=>!!(n=u.call({lexer:this},e,t))&&(e=e.substring(n.raw.length),t.push(n),!0))))if(n=this.tokenizer.escape(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.tag(e))e=e.substring(n.raw.length),i=t[t.length-1],i&&n.type==="text"&&i.type==="text"?(i.raw+=n.raw,i.text+=n.text):t.push(n);else if(n=this.tokenizer.link(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.reflink(e,this.tokens.links))e=e.substring(n.raw.length),i=t[t.length-1],i&&n.type==="text"&&i.type==="text"?(i.raw+=n.raw,i.text+=n.text):t.push(n);else if(n=this.tokenizer.emStrong(e,c,a))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.codespan(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.br(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.del(e))e=e.substring(n.raw.length),t.push(n);else if(n=this.tokenizer.autolink(e,H))e=e.substring(n.raw.length),t.push(n);else if(this.state.inLink||!(n=this.tokenizer.url(e,H))){if(s=e,this.options.extensions&&this.options.extensions.startInline){let u=1/0;const g=e.slice(1);let d;this.options.extensions.startInline.forEach(function(w){d=w.call({lexer:this},g),typeof d=="number"&&d>=0&&(u=Math.min(u,d))}),u<1/0&&u>=0&&(s=e.substring(0,u+1))}if(n=this.tokenizer.inlineText(s,ct))e=e.substring(n.raw.length),n.raw.slice(-1)!=="_"&&(a=n.raw.slice(-1)),l=!0,i=t[t.length-1],i&&i.type==="text"?(i.raw+=n.raw,i.text+=n.text):t.push(n);else if(e){const u="Infinite loop on byte: "+e.charCodeAt(0);if(this.options.silent){console.error(u);break}throw new Error(u)}}else e=e.substring(n.raw.length),t.push(n);return t}}class N{constructor(e){this.options=e||E}code(e,t,n){const i=(t||"").match(/\S*/)[0];if(this.options.highlight){const s=this.options.highlight(e,i);s!=null&&s!==e&&(n=!0,e=s)}return e=e.replace(/\n$/,"")+`
`,i?'<pre><code class="'+this.options.langPrefix+x(i,!0)+'">'+(n?e:x(e,!0))+`</code></pre>
`:"<pre><code>"+(n?e:x(e,!0))+`</code></pre>
`}blockquote(e){return`<blockquote>
`+e+`</blockquote>
`}html(e){return e}heading(e,t,n,i){return this.options.headerIds?"<h"+t+' id="'+this.options.headerPrefix+i.slug(n)+'">'+e+"</h"+t+`>
`:"<h"+t+">"+e+"</h"+t+`>
`}hr(){return this.options.xhtml?`<hr/>
`:`<hr>
`}list(e,t,n){const i=t?"ol":"ul";return"<"+i+(t&&n!==1?' start="'+n+'"':"")+`>
`+e+"</"+i+`>
`}listitem(e){return"<li>"+e+`</li>
`}checkbox(e){return"<input "+(e?'checked="" ':"")+'disabled="" type="checkbox"'+(this.options.xhtml?" /":"")+"> "}paragraph(e){return"<p>"+e+`</p>
`}table(e,t){return t&&(t="<tbody>"+t+"</tbody>"),`<table>
<thead>
`+e+`</thead>
`+t+`</table>
`}tablerow(e){return`<tr>
`+e+`</tr>
`}tablecell(e,t){const n=t.header?"th":"td";return(t.align?"<"+n+' align="'+t.align+'">':"<"+n+">")+e+"</"+n+`>
`}strong(e){return"<strong>"+e+"</strong>"}em(e){return"<em>"+e+"</em>"}codespan(e){return"<code>"+e+"</code>"}br(){return this.options.xhtml?"<br/>":"<br>"}del(e){return"<del>"+e+"</del>"}link(e,t,n){if((e=U(this.options.sanitize,this.options.baseUrl,e))===null)return n;let i='<a href="'+x(e)+'"';return t&&(i+=' title="'+t+'"'),i+=">"+n+"</a>",i}image(e,t,n){if((e=U(this.options.sanitize,this.options.baseUrl,e))===null)return n;let i='<img src="'+e+'" alt="'+n+'"';return t&&(i+=' title="'+t+'"'),i+=this.options.xhtml?"/>":">",i}text(e){return e}}class K{strong(e){return e}em(e){return e}codespan(e){return e}del(e){return e}html(e){return e}text(e){return e}link(e,t,n){return""+n}image(e,t,n){return""+n}br(){return""}}class W{constructor(){this.seen={}}serialize(e){return e.toLowerCase().trim().replace(/<[!\/a-z].*?>/gi,"").replace(/[\u2000-\u206F\u2E00-\u2E7F\\'!"#$%&()*+,./:;<=>?@[\]^`{|}~]/g,"").replace(/\s/g,"-")}getNextSafeSlug(e,t){let n=e,i=0;if(this.seen.hasOwnProperty(n)){i=this.seen[e];do i++,n=e+"-"+i;while(this.seen.hasOwnProperty(n))}return t||(this.seen[e]=i,this.seen[n]=0),n}slug(e,t={}){const n=this.serialize(e);return this.getNextSafeSlug(n,t.dryrun)}}class z{constructor(e){this.options=e||E,this.options.renderer=this.options.renderer||new N,this.renderer=this.options.renderer,this.renderer.options=this.options,this.textRenderer=new K,this.slugger=new W}static parse(e,t){return new z(t).parse(e)}static parseInline(e,t){return new z(t).parseInline(e)}parse(e,t=!0){let n,i,s,r,l,a,c,u,g,d,w,A,y,T,f,$,I,S,L,b="";const C=e.length;for(n=0;n<C;n++)if(d=e[n],this.options.extensions&&this.options.extensions.renderers&&this.options.extensions.renderers[d.type]&&(L=this.options.extensions.renderers[d.type].call({parser:this},d),L!==!1||!["space","hr","heading","code","table","blockquote","list","html","paragraph","text"].includes(d.type)))b+=L||"";else switch(d.type){case"space":continue;case"hr":b+=this.renderer.hr();continue;case"heading":b+=this.renderer.heading(this.parseInline(d.tokens),d.depth,J(this.parseInline(d.tokens,this.textRenderer)),this.slugger);continue;case"code":b+=this.renderer.code(d.text,d.lang,d.escaped);continue;case"table":for(u="",c="",r=d.header.length,i=0;i<r;i++)c+=this.renderer.tablecell(this.parseInline(d.header[i].tokens),{header:!0,align:d.align[i]});for(u+=this.renderer.tablerow(c),g="",r=d.rows.length,i=0;i<r;i++){for(a=d.rows[i],c="",l=a.length,s=0;s<l;s++)c+=this.renderer.tablecell(this.parseInline(a[s].tokens),{header:!1,align:d.align[s]});g+=this.renderer.tablerow(c)}b+=this.renderer.table(u,g);continue;case"blockquote":g=this.parse(d.tokens),b+=this.renderer.blockquote(g);continue;case"list":for(w=d.ordered,A=d.start,y=d.loose,r=d.items.length,g="",i=0;i<r;i++)f=d.items[i],$=f.checked,I=f.task,T="",f.task&&(S=this.renderer.checkbox($),y?f.tokens.length>0&&f.tokens[0].type==="paragraph"?(f.tokens[0].text=S+" "+f.tokens[0].text,f.tokens[0].tokens&&f.tokens[0].tokens.length>0&&f.tokens[0].tokens[0].type==="text"&&(f.tokens[0].tokens[0].text=S+" "+f.tokens[0].tokens[0].text)):f.tokens.unshift({type:"text",text:S}):T+=S),T+=this.parse(f.tokens,y),g+=this.renderer.listitem(T,I,$);b+=this.renderer.list(g,w,A);continue;case"html":b+=this.renderer.html(d.text);continue;case"paragraph":b+=this.renderer.paragraph(this.parseInline(d.tokens));continue;case"text":for(g=d.tokens?this.parseInline(d.tokens):d.text;n+1<C&&e[n+1].type==="text";)d=e[++n],g+=`
`+(d.tokens?this.parseInline(d.tokens):d.text);b+=t?this.renderer.paragraph(g):g;continue;default:{const R='Token with "'+d.type+'" type was not found.';if(this.options.silent)return void console.error(R);throw new Error(R)}}return b}parseInline(e,t){t=t||this.renderer;let n,i,s,r="";const l=e.length;for(n=0;n<l;n++)if(i=e[n],this.options.extensions&&this.options.extensions.renderers&&this.options.extensions.renderers[i.type]&&(s=this.options.extensions.renderers[i.type].call({parser:this},i),s!==!1||!["escape","html","link","image","strong","em","codespan","br","del","text"].includes(i.type)))r+=s||"";else switch(i.type){case"escape":r+=t.text(i.text);break;case"html":r+=t.html(i.text);break;case"link":r+=t.link(i.href,i.title,this.parseInline(i.tokens,t));break;case"image":r+=t.image(i.href,i.title,i.text);break;case"strong":r+=t.strong(this.parseInline(i.tokens,t));break;case"em":r+=t.em(this.parseInline(i.tokens,t));break;case"codespan":r+=t.codespan(i.text);break;case"br":r+=t.br();break;case"del":r+=t.del(this.parseInline(i.tokens,t));break;case"text":r+=t.text(i.text);break;default:{const a='Token with "'+i.type+'" type was not found.';if(this.options.silent)return void console.error(a);throw new Error(a)}}return r}}function m(o,e,t){if(o==null)throw new Error("marked(): input parameter is undefined or null");if(typeof o!="string")throw new Error("marked(): input parameter is of type "+Object.prototype.toString.call(o)+", string expected");if(typeof e=="function"&&(t=e,e=null),Q(e=v({},m.defaults,e||{})),t){const n=e.highlight;let i;try{i=_.lex(o,e)}catch(l){return t(l)}const s=function(l){let a;if(!l)try{e.walkTokens&&m.walkTokens(i,e.walkTokens),a=z.parse(i,e)}catch(c){l=c}return e.highlight=n,l?t(l):t(null,a)};if(!n||n.length<3||(delete e.highlight,!i.length))return s();let r=0;return m.walkTokens(i,function(l){l.type==="code"&&(r++,setTimeout(()=>{n(l.text,l.lang,function(a,c){if(a)return s(a);c!=null&&c!==l.text&&(l.text=c,l.escaped=!0),r--,r===0&&s()})},0))}),void(r===0&&s())}try{const n=_.lex(o,e);return e.walkTokens&&m.walkTokens(n,e.walkTokens),z.parse(n,e)}catch(n){if(n.message+=`
Please report this to https://github.com/markedjs/marked.`,e.silent)return"<p>An error occurred:</p><pre>"+x(n.message+"",!0)+"</pre>";throw n}}m.options=m.setOptions=function(o){var e;return v(m.defaults,o),e=m.defaults,E=e,m},m.getDefaults=Y,m.defaults=E,m.use=function(...o){const e=v({},...o),t=m.defaults.extensions||{renderers:{},childTokens:{}};let n;o.forEach(i=>{if(i.extensions&&(n=!0,i.extensions.forEach(s=>{if(!s.name)throw new Error("extension name required");if(s.renderer){const r=t.renderers?t.renderers[s.name]:null;t.renderers[s.name]=r?function(...l){let a=s.renderer.apply(this,l);return a===!1&&(a=r.apply(this,l)),a}:s.renderer}if(s.tokenizer){if(!s.level||s.level!=="block"&&s.level!=="inline")throw new Error("extension level must be 'block' or 'inline'");t[s.level]?t[s.level].unshift(s.tokenizer):t[s.level]=[s.tokenizer],s.start&&(s.level==="block"?t.startBlock?t.startBlock.push(s.start):t.startBlock=[s.start]:s.level==="inline"&&(t.startInline?t.startInline.push(s.start):t.startInline=[s.start]))}s.childTokens&&(t.childTokens[s.name]=s.childTokens)})),i.renderer){const s=m.defaults.renderer||new N;for(const r in i.renderer){const l=s[r];s[r]=(...a)=>{let c=i.renderer[r].apply(s,a);return c===!1&&(c=l.apply(s,a)),c}}e.renderer=s}if(i.tokenizer){const s=m.defaults.tokenizer||new P;for(const r in i.tokenizer){const l=s[r];s[r]=(...a)=>{let c=i.tokenizer[r].apply(s,a);return c===!1&&(c=l.apply(s,a)),c}}e.tokenizer=s}if(i.walkTokens){const s=m.defaults.walkTokens;e.walkTokens=function(r){i.walkTokens.call(this,r),s&&s.call(this,r)}}n&&(e.extensions=t),m.setOptions(e)})},m.walkTokens=function(o,e){for(const t of o)switch(e.call(m,t),t.type){case"table":for(const n of t.header)m.walkTokens(n.tokens,e);for(const n of t.rows)for(const i of n)m.walkTokens(i.tokens,e);break;case"list":m.walkTokens(t.items,e);break;default:m.defaults.extensions&&m.defaults.extensions.childTokens&&m.defaults.extensions.childTokens[t.type]?m.defaults.extensions.childTokens[t.type].forEach(function(n){m.walkTokens(t[n],e)}):t.tokens&&m.walkTokens(t.tokens,e)}},m.parseInline=function(o,e){if(o==null)throw new Error("marked.parseInline(): input parameter is undefined or null");if(typeof o!="string")throw new Error("marked.parseInline(): input parameter is of type "+Object.prototype.toString.call(o)+", string expected");Q(e=v({},m.defaults,e||{}));try{const t=_.lexInline(o,e);return e.walkTokens&&m.walkTokens(t,e.walkTokens),z.parseInline(t,e)}catch(t){if(t.message+=`
Please report this to https://github.com/markedjs/marked.`,e.silent)return"<p>An error occurred:</p><pre>"+x(t.message+"",!0)+"</pre>";throw t}},m.Parser=z,m.parser=z.parse,m.Renderer=N,m.TextRenderer=K,m.Lexer=_,m.lexer=_.lex,m.Tokenizer=P,m.Slugger=W,m.parse=m;const pt=()=>{let o,e,t=null;function n(){if(t&&!t.closed)t.focus();else{if(t=window.open("about:blank","reveal.js - Notes","width=1100,height=700"),t.marked=m,t.document.write(`<!--
	NOTE: You need to build the notes plugin after making changes to this file.
-->
<html lang="en">
	<head>
		<meta charset="utf-8">

		<title>reveal.js - Speaker View</title>

		<style>
			body {
				font-family: Helvetica;
				font-size: 18px;
			}

			#current-slide,
			#upcoming-slide,
			#speaker-controls {
				padding: 6px;
				box-sizing: border-box;
				-moz-box-sizing: border-box;
			}

			#current-slide iframe,
			#upcoming-slide iframe {
				width: 100%;
				height: 100%;
				border: 1px solid #ddd;
			}

			#current-slide .label,
			#upcoming-slide .label {
				position: absolute;
				top: 10px;
				left: 10px;
				z-index: 2;
			}

			#connection-status {
				position: absolute;
				top: 0;
				left: 0;
				width: 100%;
				height: 100%;
				z-index: 20;
				padding: 30% 20% 20% 20%;
				font-size: 18px;
				color: #222;
				background: #fff;
				text-align: center;
				box-sizing: border-box;
				line-height: 1.4;
			}

			.overlay-element {
				height: 34px;
				line-height: 34px;
				padding: 0 10px;
				text-shadow: none;
				background: rgba( 220, 220, 220, 0.8 );
				color: #222;
				font-size: 14px;
			}

			.overlay-element.interactive:hover {
				background: rgba( 220, 220, 220, 1 );
			}

			#current-slide {
				position: absolute;
				width: 60%;
				height: 100%;
				top: 0;
				left: 0;
				padding-right: 0;
			}

			#upcoming-slide {
				position: absolute;
				width: 40%;
				height: 40%;
				right: 0;
				top: 0;
			}

			/* Speaker controls */
			#speaker-controls {
				position: absolute;
				top: 40%;
				right: 0;
				width: 40%;
				height: 60%;
				overflow: auto;
				font-size: 18px;
			}

				.speaker-controls-time.hidden,
				.speaker-controls-notes.hidden {
					display: none;
				}

				.speaker-controls-time .label,
				.speaker-controls-pace .label,
				.speaker-controls-notes .label {
					text-transform: uppercase;
					font-weight: normal;
					font-size: 0.66em;
					color: #666;
					margin: 0;
				}

				.speaker-controls-time, .speaker-controls-pace {
					border-bottom: 1px solid rgba( 200, 200, 200, 0.5 );
					margin-bottom: 10px;
					padding: 10px 16px;
					padding-bottom: 20px;
					cursor: pointer;
				}

				.speaker-controls-time .reset-button {
					opacity: 0;
					float: right;
					color: #666;
					text-decoration: none;
				}
				.speaker-controls-time:hover .reset-button {
					opacity: 1;
				}

				.speaker-controls-time .timer,
				.speaker-controls-time .clock {
					width: 50%;
				}

				.speaker-controls-time .timer,
				.speaker-controls-time .clock,
				.speaker-controls-time .pacing .hours-value,
				.speaker-controls-time .pacing .minutes-value,
				.speaker-controls-time .pacing .seconds-value {
					font-size: 1.9em;
				}

				.speaker-controls-time .timer {
					float: left;
				}

				.speaker-controls-time .clock {
					float: right;
					text-align: right;
				}

				.speaker-controls-time span.mute {
					opacity: 0.3;
				}

				.speaker-controls-time .pacing-title {
					margin-top: 5px;
				}

				.speaker-controls-time .pacing.ahead {
					color: blue;
				}

				.speaker-controls-time .pacing.on-track {
					color: green;
				}

				.speaker-controls-time .pacing.behind {
					color: red;
				}

				.speaker-controls-notes {
					padding: 10px 16px;
				}

				.speaker-controls-notes .value {
					margin-top: 5px;
					line-height: 1.4;
					font-size: 1.2em;
				}

			/* Layout selector */
			#speaker-layout {
				position: absolute;
				top: 10px;
				right: 10px;
				color: #222;
				z-index: 10;
			}
				#speaker-layout select {
					position: absolute;
					width: 100%;
					height: 100%;
					top: 0;
					left: 0;
					border: 0;
					box-shadow: 0;
					cursor: pointer;
					opacity: 0;

					font-size: 1em;
					background-color: transparent;

					-moz-appearance: none;
					-webkit-appearance: none;
					-webkit-tap-highlight-color: rgba(0, 0, 0, 0);
				}

				#speaker-layout select:focus {
					outline: none;
					box-shadow: none;
				}

			.clear {
				clear: both;
			}

			/* Speaker layout: Wide */
			body[data-speaker-layout="wide"] #current-slide,
			body[data-speaker-layout="wide"] #upcoming-slide {
				width: 50%;
				height: 45%;
				padding: 6px;
			}

			body[data-speaker-layout="wide"] #current-slide {
				top: 0;
				left: 0;
			}

			body[data-speaker-layout="wide"] #upcoming-slide {
				top: 0;
				left: 50%;
			}

			body[data-speaker-layout="wide"] #speaker-controls {
				top: 45%;
				left: 0;
				width: 100%;
				height: 50%;
				font-size: 1.25em;
			}

			/* Speaker layout: Tall */
			body[data-speaker-layout="tall"] #current-slide,
			body[data-speaker-layout="tall"] #upcoming-slide {
				width: 45%;
				height: 50%;
				padding: 6px;
			}

			body[data-speaker-layout="tall"] #current-slide {
				top: 0;
				left: 0;
			}

			body[data-speaker-layout="tall"] #upcoming-slide {
				top: 50%;
				left: 0;
			}

			body[data-speaker-layout="tall"] #speaker-controls {
				padding-top: 40px;
				top: 0;
				left: 45%;
				width: 55%;
				height: 100%;
				font-size: 1.25em;
			}

			/* Speaker layout: Notes only */
			body[data-speaker-layout="notes-only"] #current-slide,
			body[data-speaker-layout="notes-only"] #upcoming-slide {
				display: none;
			}

			body[data-speaker-layout="notes-only"] #speaker-controls {
				padding-top: 40px;
				top: 0;
				left: 0;
				width: 100%;
				height: 100%;
				font-size: 1.25em;
			}

			@media screen and (max-width: 1080px) {
				body[data-speaker-layout="default"] #speaker-controls {
					font-size: 16px;
				}
			}

			@media screen and (max-width: 900px) {
				body[data-speaker-layout="default"] #speaker-controls {
					font-size: 14px;
				}
			}

			@media screen and (max-width: 800px) {
				body[data-speaker-layout="default"] #speaker-controls {
					font-size: 12px;
				}
			}

		</style>
	</head>

	<body>

		<div id="connection-status">Loading speaker view...</div>

		<div id="current-slide"></div>
		<div id="upcoming-slide"><span class="overlay-element label">Upcoming</span></div>
		<div id="speaker-controls">
			<div class="speaker-controls-time">
				<h4 class="label">Time <span class="reset-button">Click to Reset</span></h4>
				<div class="clock">
					<span class="clock-value">0:00 AM</span>
				</div>
				<div class="timer">
					<span class="hours-value">00</span><span class="minutes-value">:00</span><span class="seconds-value">:00</span>
				</div>
				<div class="clear"></div>

				<h4 class="label pacing-title" style="display: none">Pacing – Time to finish current slide</h4>
				<div class="pacing" style="display: none">
					<span class="hours-value">00</span><span class="minutes-value">:00</span><span class="seconds-value">:00</span>
				</div>
			</div>

			<div class="speaker-controls-notes hidden">
				<h4 class="label">Notes</h4>
				<div class="value"></div>
			</div>
		</div>
		<div id="speaker-layout" class="overlay-element interactive">
			<span class="speaker-layout-label"></span>
			<select class="speaker-layout-dropdown"></select>
		</div>

		<script>

			(function() {

				var notes,
					notesValue,
					currentState,
					currentSlide,
					upcomingSlide,
					layoutLabel,
					layoutDropdown,
					pendingCalls = {},
					lastRevealApiCallId = 0,
					connected = false

				var connectionStatus = document.querySelector( '#connection-status' );

				var SPEAKER_LAYOUTS = {
					'default': 'Default',
					'wide': 'Wide',
					'tall': 'Tall',
					'notes-only': 'Notes only'
				};

				setupLayout();

				let openerOrigin;

				try {
					openerOrigin = window.opener.location.origin;
				}
				catch ( error ) { console.warn( error ) }

				// In order to prevent XSS, the speaker view will only run if its
				// opener has the same origin as itself
				if( window.location.origin !== openerOrigin ) {
					connectionStatus.innerHTML = 'Cross origin error.<br>The speaker window can only be opened from the same origin.';
					return;
				}

				var connectionTimeout = setTimeout( function() {
					connectionStatus.innerHTML = 'Error connecting to main window.<br>Please try closing and reopening the speaker view.';
				}, 5000 );

				window.addEventListener( 'message', function( event ) {

					clearTimeout( connectionTimeout );
					connectionStatus.style.display = 'none';

					var data = JSON.parse( event.data );

					// The overview mode is only useful to the reveal.js instance
					// where navigation occurs so we don't sync it
					if( data.state ) delete data.state.overview;

					// Messages sent by the notes plugin inside of the main window
					if( data && data.namespace === 'reveal-notes' ) {
						if( data.type === 'connect' ) {
							handleConnectMessage( data );
						}
						else if( data.type === 'state' ) {
							handleStateMessage( data );
						}
						else if( data.type === 'return' ) {
							pendingCalls[data.callId](data.result);
							delete pendingCalls[data.callId];
						}
					}
					// Messages sent by the reveal.js inside of the current slide preview
					else if( data && data.namespace === 'reveal' ) {
						if( /ready/.test( data.eventName ) ) {
							// Send a message back to notify that the handshake is complete
							window.opener.postMessage( JSON.stringify({ namespace: 'reveal-notes', type: 'connected'} ), '*' );
						}
						else if( /slidechanged|fragmentshown|fragmenthidden|paused|resumed/.test( data.eventName ) && currentState !== JSON.stringify( data.state ) ) {

							dispatchStateToMainWindow( data.state );

						}
					}

				} );

				/**
				 * Updates the presentation in the main window to match the state
				 * of the presentation in the notes window.
				 */
				const dispatchStateToMainWindow = debounce(( state ) => {
					window.opener.postMessage( JSON.stringify({ method: 'setState', args: [ state ]} ), '*' );
				}, 500);

				/**
				 * Asynchronously calls the Reveal.js API of the main frame.
				 */
				function callRevealApi( methodName, methodArguments, callback ) {

					var callId = ++lastRevealApiCallId;
					pendingCalls[callId] = callback;
					window.opener.postMessage( JSON.stringify( {
						namespace: 'reveal-notes',
						type: 'call',
						callId: callId,
						methodName: methodName,
						arguments: methodArguments
					} ), '*' );

				}

				/**
				 * Called when the main window is trying to establish a
				 * connection.
				 */
				function handleConnectMessage( data ) {

					if( connected === false ) {
						connected = true;

						setupIframes( data );
						setupKeyboard();
						setupNotes();
						setupTimer();
						setupHeartbeat();
					}

				}

				/**
				 * Called when the main window sends an updated state.
				 */
				function handleStateMessage( data ) {

					// Store the most recently set state to avoid circular loops
					// applying the same state
					currentState = JSON.stringify( data.state );

					// No need for updating the notes in case of fragment changes
					if ( data.notes ) {
						notes.classList.remove( 'hidden' );
						notesValue.style.whiteSpace = data.whitespace;
						if( data.markdown ) {
							notesValue.innerHTML = marked( data.notes );
						}
						else {
							notesValue.innerHTML = data.notes;
						}
					}
					else {
						notes.classList.add( 'hidden' );
					}

					// Update the note slides
					currentSlide.contentWindow.postMessage( JSON.stringify({ method: 'setState', args: [ data.state ] }), '*' );
					upcomingSlide.contentWindow.postMessage( JSON.stringify({ method: 'setState', args: [ data.state ] }), '*' );
					upcomingSlide.contentWindow.postMessage( JSON.stringify({ method: 'next' }), '*' );

				}

				// Limit to max one state update per X ms
				handleStateMessage = debounce( handleStateMessage, 200 );

				/**
				 * Forward keyboard events to the current slide window.
				 * This enables keyboard events to work even if focus
				 * isn't set on the current slide iframe.
				 *
				 * Block F5 default handling, it reloads and disconnects
				 * the speaker notes window.
				 */
				function setupKeyboard() {

					document.addEventListener( 'keydown', function( event ) {
						if( event.keyCode === 116 || ( event.metaKey && event.keyCode === 82 ) ) {
							event.preventDefault();
							return false;
						}
						currentSlide.contentWindow.postMessage( JSON.stringify({ method: 'triggerKey', args: [ event.keyCode ] }), '*' );
					} );

				}

				/**
				 * Creates the preview iframes.
				 */
				function setupIframes( data ) {

					var params = [
						'receiver',
						'progress=false',
						'history=false',
						'transition=none',
						'autoSlide=0',
						'backgroundTransition=none'
					].join( '&' );

					var urlSeparator = /\\?/.test(data.url) ? '&' : '?';
					var hash = '#/' + data.state.indexh + '/' + data.state.indexv;
					var currentURL = data.url + urlSeparator + params + '&postMessageEvents=true' + hash;
					var upcomingURL = data.url + urlSeparator + params + '&controls=false' + hash;

					currentSlide = document.createElement( 'iframe' );
					currentSlide.setAttribute( 'width', 1280 );
					currentSlide.setAttribute( 'height', 1024 );
					currentSlide.setAttribute( 'src', currentURL );
					document.querySelector( '#current-slide' ).appendChild( currentSlide );

					upcomingSlide = document.createElement( 'iframe' );
					upcomingSlide.setAttribute( 'width', 640 );
					upcomingSlide.setAttribute( 'height', 512 );
					upcomingSlide.setAttribute( 'src', upcomingURL );
					document.querySelector( '#upcoming-slide' ).appendChild( upcomingSlide );

				}

				/**
				 * Setup the notes UI.
				 */
				function setupNotes() {

					notes = document.querySelector( '.speaker-controls-notes' );
					notesValue = document.querySelector( '.speaker-controls-notes .value' );

				}

				/**
				 * We send out a heartbeat at all times to ensure we can
				 * reconnect with the main presentation window after reloads.
				 */
				function setupHeartbeat() {

					setInterval( () => {
						window.opener.postMessage( JSON.stringify({ namespace: 'reveal-notes', type: 'heartbeat'} ), '*' );
					}, 1000 );

				}

				function getTimings( callback ) {

					callRevealApi( 'getSlidesAttributes', [], function ( slideAttributes ) {
						callRevealApi( 'getConfig', [], function ( config ) {
							var totalTime = config.totalTime;
							var minTimePerSlide = config.minimumTimePerSlide || 0;
							var defaultTiming = config.defaultTiming;
							if ((defaultTiming == null) && (totalTime == null)) {
								callback(null);
								return;
							}
							// Setting totalTime overrides defaultTiming
							if (totalTime) {
								defaultTiming = 0;
							}
							var timings = [];
							for ( var i in slideAttributes ) {
								var slide = slideAttributes[ i ];
								var timing = defaultTiming;
								if( slide.hasOwnProperty( 'data-timing' )) {
									var t = slide[ 'data-timing' ];
									timing = parseInt(t);
									if( isNaN(timing) ) {
										console.warn("Could not parse timing '" + t + "' of slide " + i + "; using default of " + defaultTiming);
										timing = defaultTiming;
									}
								}
								timings.push(timing);
							}
							if ( totalTime ) {
								// After we've allocated time to individual slides, we summarize it and
								// subtract it from the total time
								var remainingTime = totalTime - timings.reduce( function(a, b) { return a + b; }, 0 );
								// The remaining time is divided by the number of slides that have 0 seconds
								// allocated at the moment, giving the average time-per-slide on the remaining slides
								var remainingSlides = (timings.filter( function(x) { return x == 0 }) ).length
								var timePerSlide = Math.round( remainingTime / remainingSlides, 0 )
								// And now we replace every zero-value timing with that average
								timings = timings.map( function(x) { return (x==0 ? timePerSlide : x) } );
							}
							var slidesUnderMinimum = timings.filter( function(x) { return (x < minTimePerSlide) } ).length
							if ( slidesUnderMinimum ) {
								message = "The pacing time for " + slidesUnderMinimum + " slide(s) is under the configured minimum of " + minTimePerSlide + " seconds. Check the data-timing attribute on individual slides, or consider increasing the totalTime or minimumTimePerSlide configuration options (or removing some slides).";
								alert(message);
							}
							callback( timings );
						} );
					} );

				}

				/**
				 * Return the number of seconds allocated for presenting
				 * all slides up to and including this one.
				 */
				function getTimeAllocated( timings, callback ) {

					callRevealApi( 'getSlidePastCount', [], function ( currentSlide ) {
						var allocated = 0;
						for (var i in timings.slice(0, currentSlide + 1)) {
							allocated += timings[i];
						}
						callback( allocated );
					} );

				}

				/**
				 * Create the timer and clock and start updating them
				 * at an interval.
				 */
				function setupTimer() {

					var start = new Date(),
					timeEl = document.querySelector( '.speaker-controls-time' ),
					clockEl = timeEl.querySelector( '.clock-value' ),
					hoursEl = timeEl.querySelector( '.hours-value' ),
					minutesEl = timeEl.querySelector( '.minutes-value' ),
					secondsEl = timeEl.querySelector( '.seconds-value' ),
					pacingTitleEl = timeEl.querySelector( '.pacing-title' ),
					pacingEl = timeEl.querySelector( '.pacing' ),
					pacingHoursEl = pacingEl.querySelector( '.hours-value' ),
					pacingMinutesEl = pacingEl.querySelector( '.minutes-value' ),
					pacingSecondsEl = pacingEl.querySelector( '.seconds-value' );

					var timings = null;
					getTimings( function ( _timings ) {

						timings = _timings;
						if (_timings !== null) {
							pacingTitleEl.style.removeProperty('display');
							pacingEl.style.removeProperty('display');
						}

						// Update once directly
						_updateTimer();

						// Then update every second
						setInterval( _updateTimer, 1000 );

					} );


					function _resetTimer() {

						if (timings == null) {
							start = new Date();
							_updateTimer();
						}
						else {
							// Reset timer to beginning of current slide
							getTimeAllocated( timings, function ( slideEndTimingSeconds ) {
								var slideEndTiming = slideEndTimingSeconds * 1000;
								callRevealApi( 'getSlidePastCount', [], function ( currentSlide ) {
									var currentSlideTiming = timings[currentSlide] * 1000;
									var previousSlidesTiming = slideEndTiming - currentSlideTiming;
									var now = new Date();
									start = new Date(now.getTime() - previousSlidesTiming);
									_updateTimer();
								} );
							} );
						}

					}

					timeEl.addEventListener( 'click', function() {
						_resetTimer();
						return false;
					} );

					function _displayTime( hrEl, minEl, secEl, time) {

						var sign = Math.sign(time) == -1 ? "-" : "";
						time = Math.abs(Math.round(time / 1000));
						var seconds = time % 60;
						var minutes = Math.floor( time / 60 ) % 60 ;
						var hours = Math.floor( time / ( 60 * 60 )) ;
						hrEl.innerHTML = sign + zeroPadInteger( hours );
						if (hours == 0) {
							hrEl.classList.add( 'mute' );
						}
						else {
							hrEl.classList.remove( 'mute' );
						}
						minEl.innerHTML = ':' + zeroPadInteger( minutes );
						if (hours == 0 && minutes == 0) {
							minEl.classList.add( 'mute' );
						}
						else {
							minEl.classList.remove( 'mute' );
						}
						secEl.innerHTML = ':' + zeroPadInteger( seconds );
					}

					function _updateTimer() {

						var diff, hours, minutes, seconds,
						now = new Date();

						diff = now.getTime() - start.getTime();

						clockEl.innerHTML = now.toLocaleTimeString( 'en-US', { hour12: true, hour: '2-digit', minute:'2-digit' } );
						_displayTime( hoursEl, minutesEl, secondsEl, diff );
						if (timings !== null) {
							_updatePacing(diff);
						}

					}

					function _updatePacing(diff) {

						getTimeAllocated( timings, function ( slideEndTimingSeconds ) {
							var slideEndTiming = slideEndTimingSeconds * 1000;

							callRevealApi( 'getSlidePastCount', [], function ( currentSlide ) {
								var currentSlideTiming = timings[currentSlide] * 1000;
								var timeLeftCurrentSlide = slideEndTiming - diff;
								if (timeLeftCurrentSlide < 0) {
									pacingEl.className = 'pacing behind';
								}
								else if (timeLeftCurrentSlide < currentSlideTiming) {
									pacingEl.className = 'pacing on-track';
								}
								else {
									pacingEl.className = 'pacing ahead';
								}
								_displayTime( pacingHoursEl, pacingMinutesEl, pacingSecondsEl, timeLeftCurrentSlide );
							} );
						} );
					}

				}

				/**
				 * Sets up the speaker view layout and layout selector.
				 */
				function setupLayout() {

					layoutDropdown = document.querySelector( '.speaker-layout-dropdown' );
					layoutLabel = document.querySelector( '.speaker-layout-label' );

					// Render the list of available layouts
					for( var id in SPEAKER_LAYOUTS ) {
						var option = document.createElement( 'option' );
						option.setAttribute( 'value', id );
						option.textContent = SPEAKER_LAYOUTS[ id ];
						layoutDropdown.appendChild( option );
					}

					// Monitor the dropdown for changes
					layoutDropdown.addEventListener( 'change', function( event ) {

						setLayout( layoutDropdown.value );

					}, false );

					// Restore any currently persisted layout
					setLayout( getLayout() );

				}

				/**
				 * Sets a new speaker view layout. The layout is persisted
				 * in local storage.
				 */
				function setLayout( value ) {

					var title = SPEAKER_LAYOUTS[ value ];

					layoutLabel.innerHTML = 'Layout' + ( title ? ( ': ' + title ) : '' );
					layoutDropdown.value = value;

					document.body.setAttribute( 'data-speaker-layout', value );

					// Persist locally
					if( supportsLocalStorage() ) {
						window.localStorage.setItem( 'reveal-speaker-layout', value );
					}

				}

				/**
				 * Returns the ID of the most recently set speaker layout
				 * or our default layout if none has been set.
				 */
				function getLayout() {

					if( supportsLocalStorage() ) {
						var layout = window.localStorage.getItem( 'reveal-speaker-layout' );
						if( layout ) {
							return layout;
						}
					}

					// Default to the first record in the layouts hash
					for( var id in SPEAKER_LAYOUTS ) {
						return id;
					}

				}

				function supportsLocalStorage() {

					try {
						localStorage.setItem('test', 'test');
						localStorage.removeItem('test');
						return true;
					}
					catch( e ) {
						return false;
					}

				}

				function zeroPadInteger( num ) {

					var str = '00' + parseInt( num );
					return str.substring( str.length - 2 );

				}

				/**
				 * Limits the frequency at which a function can be called.
				 */
				function debounce( fn, ms ) {

					var lastTime = 0,
						timeout;

					return function() {

						var args = arguments;
						var context = this;

						clearTimeout( timeout );

						var timeSinceLastCall = Date.now() - lastTime;
						if( timeSinceLastCall > ms ) {
							fn.apply( context, args );
							lastTime = Date.now();
						}
						else {
							timeout = setTimeout( function() {
								fn.apply( context, args );
								lastTime = Date.now();
							}, ms - timeSinceLastCall );
						}

					}

				}

			})();

		<\/script>
	</body>
</html>`),!t)return void alert("Speaker view popup failed to open. Please make sure popups are allowed and reopen the speaker view.");(function(){const l=e.getConfig().url,a=typeof l=="string"?l:window.location.protocol+"//"+window.location.host+window.location.pathname+window.location.search;o=setInterval(function(){t.postMessage(JSON.stringify({namespace:"reveal-notes",type:"connect",state:e.getState(),url:a}),"*")},500),window.addEventListener("message",s)})()}}function i(l){let a=e.getCurrentSlide(),c=a.querySelectorAll("aside.notes"),u=a.querySelector(".current-fragment"),g={namespace:"reveal-notes",type:"state",notes:"",markdown:!1,whitespace:"normal",state:e.getState()};if(a.hasAttribute("data-notes")&&(g.notes=a.getAttribute("data-notes"),g.whitespace="pre-wrap"),u){let d=u.querySelector("aside.notes");d?(g.notes=d.innerHTML,g.markdown=typeof d.getAttribute("data-markdown")=="string",c=null):u.hasAttribute("data-notes")&&(g.notes=u.getAttribute("data-notes"),g.whitespace="pre-wrap",c=null)}c&&(g.notes=Array.from(c).map(d=>d.innerHTML).join(`
`),g.markdown=c[0]&&typeof c[0].getAttribute("data-markdown")=="string"),t.postMessage(JSON.stringify(g),"*")}function s(l){if(function(a){try{return window.location.origin===a.source.location.origin}catch{return!1}}(l)){let a=JSON.parse(l.data);a&&a.namespace==="reveal-notes"&&a.type==="connected"?(clearInterval(o),r()):a&&a.namespace==="reveal-notes"&&a.type==="call"&&function(c,u,g){let d=e[c].apply(e,u);t.postMessage(JSON.stringify({namespace:"reveal-notes",type:"return",result:d,callId:g}),"*")}(a.methodName,a.arguments,a.callId)}}function r(){e.on("slidechanged",i),e.on("fragmentshown",i),e.on("fragmenthidden",i),e.on("overviewhidden",i),e.on("overviewshown",i),e.on("paused",i),e.on("resumed",i),i()}return{id:"notes",init:function(l){e=l,/receiver/i.test(window.location.search)||(window.location.search.match(/(\?|\&)notes/gi)!==null?n():window.addEventListener("message",a=>{if(!t&&typeof a.data=="string"){let u;try{u=JSON.parse(a.data)}catch{}u&&u.namespace==="reveal-notes"&&u.type==="heartbeat"&&(c=a.source,t&&!t.closed?t.focus():(t=c,window.addEventListener("message",s),r()))}var c}),e.addKeyBinding({keyCode:83,key:"S",description:"Speaker notes view"},function(){n()}))},open:n}};export{pt as default};
