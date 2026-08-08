# onep1us 的博客

一个简约的个人博客，基于 [Hexo](https://hexo.io) + [hexo-theme-cactus](https://github.com/probberechts/hexo-theme-cactus)（dark 配色），
部署在 [GitHub Pages](https://pages.github.com/)，访问地址：<https://onep1us.github.io>。

## 写文章

在 `source/_posts/` 目录下新建 Markdown 文件，开头加上 frontmatter：

```markdown
---
title: 文章标题
date: 2026-08-08 10:00:00
categories: [分类]
tags: [标签]
---

正文内容...
```

推送代码到 `main` 分支后，GitHub Actions 会自动构建并发布。

## 本地预览

```bash
npm install
npx hexo server
```

然后打开 <http://localhost:4000>。

## 常用命令

- `npx hexo generate` — 生成静态站点到 `public/`
- `npx hexo clean` — 清除生成缓存

## 站点结构

- `_config.yml` — 站点配置（标题、URL、主题、RSS 等）
- `theme_config:`（在 `_config.yml` 内）— cactus 主题配置（配色、导航、社交链接）
- `themes/cactus/` — 主题源码
- `source/_posts/` — 文章
- `source/about/` — 关于页
