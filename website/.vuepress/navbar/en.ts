import { navbar } from "vuepress-theme-hope";

export const enNavbar = navbar([
  "/",
  {
    text: "Documents",
    link: "/en/documents/introduce",
    activeMatch: "^/en/documents",
  },
  {
    text: "Contributing",
    link: "/en/contribute/contribute",
    activeMatch: "^/en/contribute",
  },
  {
    text: "Team",
    link: "/en/team/team",
    activeMatch: "^/en/team",
  },
  {
    text: "User cases",
    link: "/en/usercases/case",
    activeMatch: "^/en/usercases",
  },
  {
    text: "Download",
    link: "/en/release/release",
    activeMatch: "^/en/release",
  },
  {
    text: "Github",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/en/github",
  }
]);
