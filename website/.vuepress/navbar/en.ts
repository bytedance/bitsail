import { navbar } from "vuepress-theme-hope";

export const enNavbar = navbar([
  "/",
  {
    text: "Documents",
    link: "/en/documents/introduce",
    activeMatch: "^/en/documents",
  },
  {
    text: "Community",
    link: "/en/community/community",
    activeMatch: "^/en/community",
  },
  {
    text: "Blog",
    link: "/en/blog/blog",
    activeMatch: "^/en/blog",
  },
  {
    text: "Download",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/en/release",
  },
  {
    text: "Github",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/en/github",
  }
]);
