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

## 5. 固定超级管理员\n\n在部署平台环境变量中设置：\n\n```env\nADMIN_ID=你的Telegram数字用户ID\n```\n\n`ADMIN_ID` 是必填项，填写数字用户 ID，不是用户名，也不要加 `@`。只有这个账号拥有超级管理员权限。普通重启、重新部署或数据库重建都不会改变管理员；需要更换管理员时，修改 `ADMIN_ID` 后重新部署/重启即可。\n\n## 6. 自动扫描已有频道历史

如果主、备份频道里已经有大量旧资源，不需要逐个转发：

1. 用一个普通 Telegram 用户账号加入主存储频道和备份频道，确保能查看历史消息（不需要设为管理员）。
2. 在 VPS 项目目录运行：

```bash
docker compose exec member-cloud-bot python login_scanner.py
```

如果使用其他部署平台，在应用终端/控制台运行 `python login_scanner.py`。按提示输入普通账号手机号、Telegram 登录验证码；开启两步验证时还需要输入密码。登录成功后会在数据库目录生成 `member_cloud_scanner.session`。
3. 确保 `DATABASE_PATH` 所在目录有持久化磁盘。Compose 已将 `./data` 挂载到 `/app/data`。
4. 重启机器人，在 Telegram 私聊机器人发送 `/scan`。
5. 用 `/scanstatus` 查看进度；用 `/scanstop` 暂停，之后再次发送 `/scan` 会从保存的位置继续。

扫描账号只读取历史消息，不会下载、转发或删除原文件。主、备份频道通过 Telegram 文件唯一标识进行配对；两边都存在的文件才会登记到网页目录。未匹配的单边文件会跳过。登录会话文件相当于账号钥匙，请勿公开或提交到 GitHub。

## 7. 常见问题

- 无法识别会员：在机器人管理面板中重新绑定会员群，确认机器人已加入会员群且能查询成员状态。
- 主/备份保存失败：确认机器人在两个私有频道中均为管理员，并有发送消息权限。
- 分享链接打不开：确认 `BOT_USERNAME` 不带 @，并且用户已加入会员群。
- 不要把数据库放在容器临时文件系统；Compose 已将 `./data` 挂载到容器。
