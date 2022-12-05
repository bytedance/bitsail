import { navbar } from "vuepress-theme-hope";

export const zhNavbar = navbar([
  "/zh/",
  {
    text: "文档",
    link: "/zh/documents/introduce",
    activeMatch: "^/zh/documents",
  },
  {
    text: "如何贡献",
    link: "/zh/contribute/contribute",
    activeMatch: "^/zh/contribute",
  },
  {
    text: "团队",
    link: "/zh/team/team",
    activeMatch: "^/zh/team",
  },
  {
    text: "用户案例",
    link: "/zh/usercases/case",
    activeMatch: "^/zh/usercases",
  },
  {
    text: "下载",
    link: "/zh/release/release",
    activeMatch: "^/zh/release",
  },
  {
    text: "Github",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/zh/github",
  }
]);
