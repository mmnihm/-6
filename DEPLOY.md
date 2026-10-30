# VPS 部署（Docker Compose）

## 1. 准备

VPS 安装 Docker Engine 和 Docker Compose 插件，并确保机器人已经加入会员群、主存储频道和备份频道。

## 2. 获取项目

```bash
git clone https://github.com/mmnihm/-6.git
cd -- -6
cp .env.example .env
nano .env
```

在 `.env` 中填写真实变量。不要把真实 Token、API_HASH 或频道信息提交到 GitHub。

## 3. 启动

```bash
docker compose up -d --build
docker compose logs -f --tail=100
```

停止：

```bash
docker compose down
```

更新代码：

```bash
git pull
docker compose up -d --build
```

## 4. 网页目录访问

部署后，平台分配的 HTTPS 域名加上 `/catalog` 即为公开目录链接，例如：

```
https://你的部署域名/catalog
```

自建 VPS 可将容器的 8080 端口通过 Nginx/Caddy 反向代理并配置 HTTPS。目录对所有访客开放；文件取回仍由 Telegram 机器人验证会员资格。网页服务默认读取平台的 `PORT`，没有该变量时使用 `WEB_PORT`（默认 8080）。

## 5. 首次管理员认领顺序

1. 先启动机器人。
2. 立即由部署者本人打开机器人私聊并发送 `/start`。
3. 确认收到“首位管理员认领成功”。
4. 再把机器人用户名提供给会员。

首次认领会写入 SQLite 的 `settings.superadmin_id`，普通重启不会重置。请备份 `data/bot.db`。

## 6. 常见问题

- 无法识别会员：在机器人管理面板中重新绑定会员群，确认机器人已加入会员群且能查询成员状态。
- 主/备份保存失败：确认机器人在两个私有频道中均为管理员，并有发送消息权限。
- 分享链接打不开：确认 `BOT_USERNAME` 不带 @，并且用户已加入会员群。
- 不要把数据库放在容器临时文件系统；Compose 已将 `./data` 挂载到容器。
