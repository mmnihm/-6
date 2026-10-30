# 部署说明（EthioDeploy / VPS）

## EthioDeploy（推荐使用托管 PostgreSQL）

1. 在项目的「插件 / Addons」中添加 PostgreSQL，等待状态显示准备就绪。
2. EthioDeploy 会自动向应用注入 `DATABASE_URL`，不要把连接串或密码复制到 GitHub，也不需要手动添加同名环境变量。
3. 确认项目环境变量仍包含 `BOT_TOKEN`、`API_ID`、`API_HASH`、`ADMIN_ID`。推送到 GitHub `main` 后平台会自动重新部署。
4. 应用启动时会自动创建 PostgreSQL 表。如果当前容器中仍存在旧的 `data/bot.db`，程序会尝试一次性导入旧数据；导入采用冲突跳过，不覆盖 PostgreSQL 已有记录。
5. 若旧 SQLite 文件已随此前部署被平台清除，程序无法凭空恢复旧数据；需要从平台旧部署、数据库备份或其他副本恢复。

**重要：** PostgreSQL 保存会员、绑定、文件目录和扫描进度。Pyrogram 扫描账号的 `.session` 仍是本地独立文件；EthioDeploy 若没有持久化文件存储，重新部署后可能需要重新登录扫描账号，但数据库中的扫描进度不会因此丢失。

## VPS 部署（Docker Compose）

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
2. 在机器人私聊发送 `/admin`，点击“扫描账号登录”→“开始登录”，按机器人提示输入手机号、Telegram 登录验证码及两步验证密码（如有），无需进入网站终端。
3. 扫描账号登录会话保存在本地 `.session` 文件；EthioDeploy 若无持久化文件存储，重新部署后可能需要重新登录。扫描进度保存在 PostgreSQL，不受影响。
4. 登录成功后发送 `/scan` 开始扫描；用 `/scanstatus` 查看进度；用 `/scanstop` 暂停，之后再次发送 `/scan` 会从保存的位置继续。

扫描账号只读取历史消息，不会下载、转发或删除原文件。主、备份频道通过 Telegram 文件唯一标识进行配对；两边都存在的文件才会登记到网页目录。未匹配的单边文件会跳过。登录会话文件相当于账号钥匙，请勿公开或提交到 GitHub。验证码和两步验证密码会短暂经过机器人私聊并尝试立即删除，建议使用专门的扫描账号。

## 7. 常见问题

- 无法识别会员：在机器人管理面板中重新绑定会员群，确认机器人已加入会员群且能查询成员状态。
- 主/备份保存失败：确认机器人在两个私有频道中均为管理员，并有发送消息权限。
- 分享链接打不开：确认 `BOT_USERNAME` 不带 @，并且用户已加入会员群。
- EthioDeploy 使用托管 PostgreSQL；VPS Compose 使用 `./data` 卷保存 SQLite 数据和扫描账号会话。
