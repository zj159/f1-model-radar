# F1 车模情报站

一个半自动的 F1 车模资讯网站：公开信息流、筛选搜索、详情页、投稿线索、后台发布和小红书文案生成。

公开试运营步骤见：[`PUBLIC_LAUNCH_WORKFLOW.md`](PUBLIC_LAUNCH_WORKFLOW.md)。

## 运行

```bash
cd "/Users/zhuzijie/Documents/New project/f1-model-radar"
./scripts/run_local.sh
```

打开：

- 首页：http://127.0.0.1:8000
- 后台登录：http://127.0.0.1:8000/admin/login
- 投稿：http://127.0.0.1:8000/submit
- RSS：http://127.0.0.1:8000/rss.xml

本地后台 token 已经放在 `.env`，这个文件被 `.gitignore` 忽略，不会提交到 Git。

如果部署在 HTTPS 域名下，建议同时打开安全 cookie：

```bash
F1_RADAR_COOKIE_SECURE=1
```

如果想关闭后台自动抓取：

```bash
F1_RADAR_AUTO_FETCH_INTERVAL_MINUTES=0 ./scripts/run_local.sh
```

如果部署平台支持持久化磁盘，可以用 `F1_RADAR_DB_PATH` 指向持久化路径，例如 `/var/data/radar.sqlite3`。
Render 免费档不支持持久磁盘，所以 `render.yaml` 默认使用 `/tmp/f1-model-radar.sqlite3` 先跑通公网版本；免费实例重启或重新部署后，后台数据可能会清空。

## 第一阶段用法

后台有两种发布方式：

1. 点击“抓取最新”，系统会从 `data/sources.json` 里的公开来源抓取最新条目，进入“待审核情报”，并写入抓取健康记录。
2. 你确认标题、来源、车型没问题后点“发布”，它才会出现在首页。
3. 如果某条不相关，点“忽略”。
4. 也可以继续手动发布，把 Stone Model Car、GPworld、品牌官方、海外店铺或社媒动态里的重点复制到后台。

图片可以二选一：

- 粘贴图片外链
- 在后台直接上传本地图片，支持 jpg、png、webp、gif，单张小于 6 MB

发布后，每条详情页会自动生成一份小红书文案，方便你直接拿去运营账号。

## 已补上的公开站基础能力

- 首页分页，每页 60 条。
- RSS、robots.txt、sitemap.xml。
- 后台登录 cookie，不再把 token 放在所有后台链接里。
- 后台接口只认登录 cookie，`/admin?token=...` 不再直接进入后台。
- 抓取健康记录，能看到最近抓取新增、已见和错误数。
- 抓取流程有全局锁，避免重复点击或自动抓取撞在一起。
- 发布去重，避免同一来源重复发布。
- 批量发布默认最多 20 条，最高 100 条，并需要确认。

## 后续可以加

- 邮件或 Telegram 订阅
- 车手/车队关键词提醒
- 图片本地缓存
- 更智能的中文摘要和标题改写

## 自动来源

默认来源文件会自动生成在：

```text
data/sources.json
```

当前支持：

- Shopify 产品 JSON，例如 Stone Model Car
- GPworld 这种按行列出状态、标题、规格的页面
- 通用链接页面

Facebook / Instagram 通常需要登录并且反爬，不建议第一版直接抓。更稳的方式是先抓公开店铺、品牌官网、RSS 或 newsletter 网页。
