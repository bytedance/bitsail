/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    link: "https://github.com/bytedance/bitsail/releases",
    activeMatch: "^/zh/release",
  },
  {
    text: "Github",
    link: "https://github.com/bytedance/bitsail",
    activeMatch: "^/zh/github",
  }
]);
