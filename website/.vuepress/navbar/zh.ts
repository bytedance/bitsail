import { navbar } from "vuepress-theme-hope";

export const zhNavbar = navbar([
  "/zh/",
  {
    text: "文档",
    link: "/zh/documents/introduce",
    activeMatch: "^/zh/documents",
  },
  {
    text: "社区",
    link: "/zh/community/community",
    activeMatch: "^/zh/community",
  },
  {
    text: "博客",
    link: "/zh/blog/blog",
    activeMatch: "^/zh/blog",
  },
  {
    text: "下载",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/zh/release",
  },
  {
    text: "Github",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/zh/github",
  }
]);
