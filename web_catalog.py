import html
from urllib.parse import urlencode

from aiohttp import web


PAGE_SIZE = 30


def create_catalog_app(database, username_provider):
    async def catalog_handler(request):
        query = (request.query.get("q") or "").strip()[:100]
        category = (request.query.get("category") or "").strip()[:40]
        try:
            page = max(1, int(request.query.get("page", "1")))
        except (TypeError, ValueError):
            page = 1

        rows, categories, total = await database.public_catalog(
            query=query, category=category,
            limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE,
        )
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if page > pages:
            page = pages
            rows, categories, total = await database.public_catalog(
                query=query, category=category,
                limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE,
            )

        username = username_provider()
        if not username:
            return web.Response(text="机器人尚未完成启动", status=503)

        def make_url(target_page):
            params = {"page": target_page}
            if query:
                params["q"] = query
            if category:
                params["category"] = category
            return "/catalog?" + urlencode(params)

        category_options = ['<option value="">全部分类</option>']
        for item in categories:
            selected = " selected" if item == category else ""
            category_options.append(
                f'<option value="{html.escape(item, quote=True)}"{selected}>'
                f'{html.escape(item)}</option>'
            )

        cards = []
        for row in rows:
            name = html.escape(row["file_name"])
            group = html.escape(row["category"] or "未分类")
            media_type = html.escape(row["media_type"])
            created = html.escape((row["created_at"] or "")[:10])
            link = f'https://t.me/{username}?start=file_{row["file_key"]}'
            cards.append(
                '<article class="file">'
                f'<div class="meta"><span>{group}</span><span>{media_type}</span></div>'
                f'<h3>{name}</h3>'
                f'<div class="bottom"><small>{created}</small>'
                f'<a href="{html.escape(link, quote=True)}">在 Telegram 取回 ↗</a></div>'
                '</article>'
            )

        if not cards:
            content = '<div class="empty">没有找到文件，试试其他关键词或分类。</div>'
        else:
            content = '<div class="files">' + "".join(cards) + '</div>'

        pager = []
        if page > 1:
            pager.append(f'<a href="{make_url(page - 1)}">← 上一页</a>')
        pager.append(f'<span>第 {page} / {pages} 页</span>')
        if page < pages:
            pager.append(f'<a href="{make_url(page + 1)}">下一页 →</a>')

        document = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>会员网盘 · 文件目录</title>
<style>
:root{{color-scheme:dark;--bg:#10151d;--panel:#192230;--muted:#9aa9bb;--line:#2a394b;--accent:#78aaff}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:#edf3fa;font:15px/1.6 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:900px;margin:auto;padding:28px 16px 50px}}header{{margin-bottom:22px}}
h1{{font-size:25px;margin:0 0 5px}}.sub{{color:var(--muted);font-size:13px}}
.notice{{margin:18px 0;padding:12px 14px;border:1px solid #344b69;border-radius:12px;background:#17263a;color:#cfe1ff}}
form{{display:grid;grid-template-columns:1fr 170px auto;gap:9px;margin:18px 0}}
input,select,button{{min-width:0;padding:12px;border:1px solid var(--line);border-radius:10px;background:var(--panel);color:#edf3fa;font:inherit}}
button{{background:#2b5d9e;border-color:#2b5d9e;font-weight:600;cursor:pointer}}
.count{{color:var(--muted);font-size:13px;margin:14px 2px}}
.files{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}
.file{{min-width:0;padding:15px;background:var(--panel);border:1px solid var(--line);border-radius:13px}}
.meta,.bottom{{display:flex;justify-content:space-between;gap:10px;align-items:center}}
.meta{{color:var(--muted);font-size:12px}}h3{{font-size:15px;overflow-wrap:anywhere;margin:12px 0 18px}}
.bottom{{font-size:12px;color:var(--muted)}}a{{color:var(--accent);text-decoration:none;font-weight:600}}
.pager{{display:flex;justify-content:center;align-items:center;gap:22px;margin:25px 0;color:var(--muted);font-size:13px}}
.empty{{padding:35px 15px;text-align:center;color:var(--muted);background:var(--panel);border-radius:12px}}
footer{{margin-top:25px;text-align:center;color:var(--muted);font-size:12px}}
@media(max-width:600px){{main{{padding:22px 12px 35px}}form{{grid-template-columns:1fr 1fr}}form input{{grid-column:1/-1}}form button{{grid-column:1/-1}}.files{{grid-template-columns:1fr}}h1{{font-size:22px}}}}
</style>
</head>
<body><main>
<header><h1>☁️ 会员网盘 · 文件目录</h1>
<div class="sub">公开浏览全部目录 · 文件取回需要会员权限</div></header>
<div class="notice">📂 这里展示文件名称与分类。点击“在 Telegram 取回”后，机器人会自动验证会员资格；非会员无法下载。</div>
<form method="get" action="/catalog">
<input type="search" name="q" value="{html.escape(query, quote=True)}" placeholder="搜索文件名称或分类">
<select name="category">{"".join(category_options)}</select>
<button type="submit">搜索目录</button>
</form>
<div class="count">共 {total} 个文件</div>
{content}
<nav class="pager">{"".join(pager)}</nav>
<footer>文件实际存储于 Telegram 私有频道，网页不会公开存储频道地址。</footer>
</main></body></html>"""
        return web.Response(text=document, content_type="text/html", charset="utf-8")

    async def root_handler(request):
        raise web.HTTPFound("/catalog")

    async def health_handler(request):
        return web.json_response({"ok": True})

    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_get("/", root_handler)
    app.router.add_get("/catalog", catalog_handler)
    app.router.add_get("/healthz", health_handler)
    return app
