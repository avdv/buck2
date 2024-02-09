"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[6568],{3905:(e,t,n)=>{n.r(t),n.d(t,{MDXContext:()=>s,MDXProvider:()=>c,mdx:()=>f,useMDXComponents:()=>d,withMDXComponents:()=>m});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(){return i=Object.assign||function(e){for(var t=1;t<arguments.length;t++){var n=arguments[t];for(var a in n)Object.prototype.hasOwnProperty.call(n,a)&&(e[a]=n[a])}return e},i.apply(this,arguments)}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function u(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),m=function(e){return function(t){var n=d(t.components);return a.createElement(e,i({},t,{components:n}))}},d=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},c=function(e){var t=d(e.components);return a.createElement(s.Provider,{value:t},e.children)},p="mdxType",h={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},b=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,o=e.parentName,s=u(e,["components","mdxType","originalType","parentName"]),m=d(n),c=r,p=m["".concat(o,".").concat(c)]||m[c]||h[c]||i;return n?a.createElement(p,l(l({ref:t},s),{},{components:n})):a.createElement(p,l({ref:t},s))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=b;var l={};for(var u in t)hasOwnProperty.call(t,u)&&(l[u]=t[u]);l.originalType=e,l[p]="string"==typeof e?e:r,o[1]=l;for(var s=2;s<i;s++)o[s]=n[s];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}b.displayName="MDXCreateElement"},87989:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>u,contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>l,toc:()=>s});var a=n(87462),r=(n(67294),n(3905));const i={id:"index",title:"Introduction"},o=void 0,l={unversionedId:"index",id:"index",title:"Introduction",description:"Welcome to Buck2, a large scale, fast, reliable, and extensible build tool",source:"@site/../docs/index.md",sourceDirName:".",slug:"/",permalink:"/docs/",draft:!1,tags:[],version:"current",frontMatter:{id:"index",title:"Introduction"},sidebar:"manualSidebar",next:{title:"Why Buck2",permalink:"/docs/why"}},u={},s=[{value:"Buck2 Documentation Website Links",id:"buck2-documentation-website-links",level:2},{value:"For end users",id:"for-end-users",level:3},{value:"For people writing rules",id:"for-people-writing-rules",level:3},{value:"For people integrating with Buck2",id:"for-people-integrating-with-buck2",level:3},{value:"External articles about Buck2",id:"external-articles-about-buck2",level:3},{value:"External videos about Buck2",id:"external-videos-about-buck2",level:3},{value:"External projects using Buck2",id:"external-projects-using-buck2",level:3},{value:"For people developing Buck2",id:"for-people-developing-buck2",level:3}],m=(d="FbInternalOnly",function(e){return console.warn("Component "+d+" was not imported, exported, or provided by MDXProvider as global scope"),(0,r.mdx)("div",e)});var d;const c={toc:s};function p(e){let{components:t,...n}=e;return(0,r.mdx)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.mdx)("p",null,"Welcome to Buck2, a large scale, fast, reliable, and extensible build tool\ndeveloped and used by Meta. Buck2 supports a variety of languages on many\nplatforms."),(0,r.mdx)("p",null,"Buck2's core is written in ",(0,r.mdx)("a",{parentName:"p",href:"https://www.rust-lang.org/"},"Rust"),".\n",(0,r.mdx)("a",{parentName:"p",href:"https://github.com/bazelbuild/starlark"},"Starlark"),", which is a deterministic,\nimmutable dialect of Python, is used to extend the Buck2 build system, enabling\nBuck2 to be language-agnostic. With Starlark, users can define their own custom\nrules."),(0,r.mdx)("p",null,"Buck2 leverages the Bazel spec of\n",(0,r.mdx)("a",{parentName:"p",href:"https://bazel.build/remote/rbe"},"Remote Build Execution")," as the primary means of\nparallelization and caching, which increases the importance of idempotency (no\nmatter how many times an operation is performed, it yields the same result) and\nhermeticity (code is sealed off from the world), giving the right results,\nreliably."),(0,r.mdx)("p",null,"Buck2 multi-language support includes C++, Python, Java, Go, Rust, Erlang,\nOCaml, and more."),(0,r.mdx)("p",null,"The following sub-sections contain a list of links to key points in the Buck2\nDocumentation website that explain the advantages of using Buck2 for you and\nyour team."),(0,r.mdx)("h2",{id:"buck2-documentation-website-links"},"Buck2 Documentation Website Links"),(0,r.mdx)("h3",{id:"for-end-users"},"For end users"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"/docs/getting_started"},"Getting Started")," - how to get started with using Buck2."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"/docs/benefits"},"Benefits")," - the benefits of using Buck2.")),(0,r.mdx)(m,{mdxType:"FbInternalOnly"},(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"users/migration_guide.fb.md"},"Migration Guide")," - how to port projects from\nBuck to Buck2, including the issues you might face and notable differences."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"users/build_observability/observability.fb.md"},"Buck2 and Build Observability")," -\nhow to use Buck2's datasets to analyze specific invocations or classes of\ninvocations."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"users/advanced/vpnless.fb.md"},"Migrating builds to work VPNless")," - how to\nmigrate builds to work without VPN or lighthouse access."))),(0,r.mdx)("h3",{id:"for-people-writing-rules"},"For people writing rules"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"/docs/rule_authors/writing_rules"},"Writing Rules")," - how to write rules to support\nnew languages."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"api/build/globals"},"Build APIs")," - documentation for the APIs available when\nwriting rules."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/facebookexperimental/starlark-rust/blob/main/docs/types.md"},"Starlark Types")," -\nrules are written in Starlark (which is approximately Python), but our version\nadds types.")),(0,r.mdx)(m,{mdxType:"FbInternalOnly"},(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"rule_authors/rule_writing_tips.fb.md"},"Rule Writing Tips")," - tips for migrating\nrules from Buck1 to Buck2."))),(0,r.mdx)("h3",{id:"for-people-integrating-with-buck2"},"For people integrating with Buck2"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"/docs/developers/bxl"},"Extending Buck via BXL")," - powerful Starlark scripts for\nintrospection of Buck2's graphs."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/facebookincubator/reindeer"},"Reindeer")," - a set of tools for\nimporting Rust crates from crates.io, git repos etc and generating a BUCK file\nfor using them."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/facebook/ocaml-scripts"},"ocaml-scripts")," - scripts to\ngenerate a BUCK file enabling the use of OCaml packages from an OPAM switch.")),(0,r.mdx)("h3",{id:"external-articles-about-buck2"},"External articles about Buck2"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://engineering.fb.com/2023/04/06/open-source/buck2-open-source-large-scale-build-system/"},"Introducing Buck2")," -\nour initial introduction when we open sourced Buck2."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://old.reddit.com/r/rust/comments/136qs44/hello_rrust_we_are_meta_engineers_who_created_the/"},"Reddit AMA"),"\nwhere the Buck2 team answered a number of questions."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://steveklabnik.com/writing/using-buck-to-build-rust-projects"},"Using buck to build Rust projects")," -\nworking through an initial small Rust project, by\n",(0,r.mdx)("a",{parentName:"li",href:"https://steveklabnik.com/"},"Steve Klabnik"),". Followed up by\n",(0,r.mdx)("a",{parentName:"li",href:"https://steveklabnik.com/writing/using-cratesio-with-buck"},"building from crates.io"),"\nand ",(0,r.mdx)("a",{parentName:"li",href:"https://steveklabnik.com/writing/updating-buck"},"updating Buck2"),"."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/sluongng/awesome-buck2"},"Awesome Buck2")," is a collection of\nresources about Buck2."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.buildbuddy.io/blog/buck2-review/"},"Buck2 Unboxing")," is a general\nreview of Buck2 by ",(0,r.mdx)("a",{parentName:"li",href:"https://github.com/sluongng/"},"Son Luong Ngoc"),"."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.tweag.io/blog/2023-07-06-buck2/"},"A tour around Buck2")," gives an\noverview of Buck2 and how it differs from Bazel.")),(0,r.mdx)("h3",{id:"external-videos-about-buck2"},"External videos about Buck2"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.youtube.com/watch?v=oMIzKVxUNAE"},"Accelerating builds with Buck2"),"\nNeil talks about why Buck2 is fast."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.youtube.com/watch?v=EQfVu42KwDs"},"Buck2: optimizations & dynamic dependencies"),"\nNeil and Chris talk about why Buck2 is fast and some of the advanced\ndependency features."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.youtube.com/watch?v=4ALgsBqNBhQ"},"Building Erlang with Buck2"),"\nAndreas talks about building WhatsApp with Buck2."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.youtube.com/watch?v=Wv-ilbckSx4"},"antlir2: Deterministic image bulids with Buck2"),"\ntalks about layering a packaging system over Buck2.")),(0,r.mdx)("h3",{id:"external-projects-using-buck2"},"External projects using Buck2"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.systeminit.com/"},"System Initiative")," build their DevOps product\n",(0,r.mdx)("a",{parentName:"li",href:"https://nickgerace.dev/post/system-initiative-the-second-wave-of-devops/#under-the-hood"},"using Buck2"),",\nwith their own custom prelude."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/dtolnay/cxx"},"Rust ",(0,r.mdx)("inlineCode",{parentName:"a"},"cxx")," library")," has examples and tests\nwith a wide variety of build systems, including Buck2."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/facebook/ocamlrep"},(0,r.mdx)("inlineCode",{parentName:"a"},"ocamlrep")," library")," allows for interop\nbetween OCaml and Rust code, and can be\n",(0,r.mdx)("a",{parentName:"li",href:"https://github.com/facebook/ocamlrep/blob/main/README-BUCK.md"},"built with Buck2"),"."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://github.com/thoughtpolice/buck2-nix"},(0,r.mdx)("inlineCode",{parentName:"a"},"buck2-nix"))," is an experiment to\nintegrate Buck2, ",(0,r.mdx)("a",{parentName:"li",href:"https://sapling-scm.com"},"Sapling")," and\n",(0,r.mdx)("a",{parentName:"li",href:"https://nixos.org"},"Nix")," together in a harmonious way.")),(0,r.mdx)("p",null,"Feel free to\n",(0,r.mdx)("a",{parentName:"p",href:"https://github.com/facebook/buck2/edit/main/docs/index.md"},"send a PR")," adding\nyour project."),(0,r.mdx)(m,{mdxType:"FbInternalOnly"},(0,r.mdx)("h3",{id:"for-people-developing-buck2"},"For people developing Buck2"),(0,r.mdx)("ul",null,(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"https://www.internalfb.com/code/fbsource/fbcode/buck2/README.md"},"Basic README")," -\nhow to get started, compile Buck2 and the basic workflows."),(0,r.mdx)("li",{parentName:"ul"},(0,r.mdx)("a",{parentName:"li",href:"developers/developers.fb.md"},"Notes for Developers")," - more advanced workflows\nand notes around debugging, profiling etc."))))}p.isMDXComponent=!0}}]);