# onep1us 的博客

一个简约的个人博客，基于 [less-style-please](https://github.com/feeshy/less-style-please) 主题，
部署在 [GitHub Pages](https://pages.github.com/)，访问地址：<https://onep1us.github.io>。

## 写文章

在 `tech/_posts/`（技术）或 `life/_posts/`（随笔）目录下新建 Markdown 文件，
文件名格式为 `YYYY-MM-DD-标题.md`，开头加上 frontmatter：

```markdown
---
layout: post
title: 文章标题
tags:
  - 标签
description: 一句话描述
---
```

推送代码后，GitHub Actions 会自动构建并发布。

## 本地预览（可选）

需要 Ruby + Bundler：

```bash
bundle install
bundle exec jekyll serve
```

然后打开 <http://localhost:4000>。

## 站点结构

- `_config.yml` — 站点配置（标题、作者、主题等）
- `_data/menu.yml` — 首页导航结构
- `tech/`、`life/` — 文章分类目录
