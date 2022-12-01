import { navbar } from "vuepress-theme-hope";

export const enNavbar = navbar([
  "/",
  {
    text: "Guide",
    link: "/start/env_setup",
    activeMatch: "^/start",
  },
  {
    text: "Components",
    link: "/components/introduction",
    activeMatch: "^/components",
  },
  {
    text: "Connectors",
    link: "/connectors/introduction",
    activeMatch: "^/connectors",
  },
]);
