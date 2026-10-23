# OKX Boost 实时看板

一个基于 Python 标准库的 OKX Boost 活动看板，提供实时链上排行、活动管理与数据查询。无需安装第三方 Python 依赖。

## 运行

需要 Python 3.6 或更高版本。

```bash
ADMIN_PASS=请替换为管理密码 SCAN_THREADS=10 python3 okx_boost_server.py
```

默认访问地址为 <http://127.0.0.1:8787>，管理后台位于 `/admin`。

可通过以下环境变量调整部署：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | 监听地址，对外服务时可设为 `0.0.0.0` |
| `PORT` | `8787` | HTTP 端口 |
| `ADMIN_PASS` | 空 | 管理后台密码 |
| `AUTH_USER` / `AUTH_PASS` | 空 | 页面基础认证 |
| `SCAN_THREADS` | `3` | 链上扫描并发数 |
| `CACHE_TTL` | `5` | OKX 接口代理缓存秒数 |
| `NO_BROWSER` | 空 | 设为 `1` 时不在启动后自动打开浏览器 |

## 文件

- `okx_boost_server.py`：HTTP 服务、活动扫描和数据更新。
- `okx-boost-live-rank.html`：实时排行页面。
- `admin.html`：管理后台。
- `activities.json`：活动配置。
- `data/`：运行时生成的数据，不纳入 Git。

首次启动会自动创建 `data/` 目录并同步官方接口及链上数据。

## 安全

不要将管理密码写入源码或提交到仓库。生产环境建议通过环境变量注入，并在反向代理层启用 HTTPS。
