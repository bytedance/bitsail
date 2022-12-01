import { navbar } from "vuepress-theme-hope";

export const zhNavbar = navbar([
  "/zh/",
  {
    text: "指南",
    link: "/zh/start/env_setup",
    activeMatch: "^/zh/start",
  },
  {
    text: "功能组件",
    link: "/zh/components/introduction",
    activeMatch: "^/zh/components",
  },
  {
    text: "连接器",
    link: "/zh/connectors/introduction",
    activeMatch: "^/zh/connectors",
  },
]);
