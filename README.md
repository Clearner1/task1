# OATD Thesis Crawler

批量采集 [OATD](https://oatd.org/)（Open Access Theses and Dissertations）上公开论文的元数据，并尽可能解析公开可访问的 PDF 直链与下载样例文件。

## 核心设计思路

### 架构：借力用户浏览器，绕过反爬

本项目**没有**使用 Playwright/Selenium 等自动化浏览器框架，而是通过 [opencli](https://github.com/jackwener/OpenCLI) 复用用户**自己的 Chrome 浏览器**进行数据采集。

**为什么这样设计？**

| 传统方案 (Playwright) | 本项目方案 (opencli + Chrome) |
|---|---|
| 需要处理 Cloudflare 检测 | 用户 Chrome 自然通过 |
| 需要 playwright-stealth 等反检测 | 无需任何反检测 |
| 需要管理浏览器进程生命周期 | Chrome 独立运行，永不崩溃 |
| 依赖重 (~300MB) | 仅需 opencli CLI |

### 数据流

```
Python 脚本 (调度 + 存储)
    │
    ├── opencli browser open <url>    → Chrome 导航到搜索页
    ├── opencli browser eval <js>     → Chrome 内执行 JS 提取数据
    │       └── 返回 JSON ──────────→ 写入 JSONL 文件
    │
    └── httpx (async)                → 直接下载 PDF（外部仓库无 CF）
```

## 运行环境

- **OS**: macOS / Linux
- **Python**: 3.10+
- **Chrome**: 已安装并运行
- **opencli**: 已安装 + Browser Bridge 扩展已启用
- **GitHub 提交形态**: 当前目录 `task1/` 可作为独立仓库直接提交

```bash
# 检查 opencli 状态
opencli doctor
```

## 运行前提

本项目依赖 `opencli + Chrome` 的浏览器桥接能力，而不是无头浏览器。运行前需要满足以下前提：

- Chrome 处于打开状态
- 已安装并启用 OpenCLI Browser Bridge 扩展
- `opencli doctor` 检查通过，daemon 与扩展连接正常
- 爬取执行期间保持本机 Chrome 会话可用，不主动关闭浏览器或禁用扩展

说明：

- 该方案的优点是可以复用真实浏览器上下文，降低 OATD 站点及外部仓储的反爬干扰
- 代价是运行环境不再是纯 headless 模式，而是依赖一个稳定的本地浏览器会话

## 安装与运行

```bash
# 1. 安装 Python 依赖
pip install -r requirements.txt

# 2. 确保 Chrome 已打开并安装了 opencli Browser Bridge 扩展

# 3. 运行完整流程（元数据采集 + PDF 下载样例）
python -m src.main

# 仅采集元数据（推荐先跑这个）
python -m src.main --crawl-only

# 仅下载 PDF 样例（需要先有元数据）
python -m src.main --pdf-only

# 为已有元数据补全 pdf_url 字段（不下载 PDF 文件）
python -m src.backfill_pdf_urls

# 查看当前进度
python -m src.main --stats

# 导出 CSV
python -m src.main --export-csv
```

## 数据输出格式

### 元数据 (JSONL)

输出文件: `data/metadata/theses.jsonl`，每行一条 JSON 记录：

```json
{
  "title": "Machine learning based approaches to stuttering detection",
  "author": "Al-Banna, Abedal-karim",
  "university": "Loughborough University",
  "year": "2023",
  "degree": "PhD, Computer Science, 2023, Loughborough University",
  "url": "https://doi.org/10.26174/thesis.lboro.24541102.v1",
  "abstract": "Machine learning based approaches to...",
  "keywords": "Deep learning; Machine Learning; Stuttering",
  "detail_url": "https://oatd.org/oatd/record?record=...",
  "record_id": "oai:figshare.com:article/24541102",
  "pdf_url": "https://repository.example.edu/thesis.pdf"
}
```

同时提供 CSV 格式: `data/metadata/theses.csv`

字段说明：

- `url`: OATD 搜索结果页给出的外部论文入口链接，一般是 DOI、Handle 或机构仓储落地页
- `pdf_url`: 从 `url` 继续解析得到的公开可访问 PDF 直链；若源站需要校园网/VPN、人工跳转、多步表单或当前不可访问，则保留为空字符串
- `detail_url`: OATD 详情页链接

说明：

- 题目要求中的 `PDF URL`，本项目以 `pdf_url` 字段交付
- 对于确实无法公开解析出直链的记录，保留 `pdf_url=""`，同时保留 `url` 作为可追溯的论文入口链接
- OATD 聚合的是大量异构机构仓储，不同学校对匿名访问、跳转方式、反爬策略和权限控制差异很大，因此 `pdf_url` 覆盖率会显著低于元数据覆盖率，这是源站可访问性差异导致的正常现象
- OATD 某些源记录本身未稳定展示作者字段，因此 `author` 在部分记录中会保留为空字符串

### PDF 文件

输出目录: `data/pdfs/`，文件名基于 record_id。

本项目默认只下载一部分 PDF 作为样例交付，而不是将 5,000+ 篇论文全部落盘。原因如下：

- 题目要求中“已下载的论文文件（或样例）”允许提交样例文件
- 5,000+ 篇论文来自不同机构仓储，单篇大小差异很大，全量下载会显著增加磁盘占用、带宽消耗和失败重试成本
- 因此本项目采用“全量元数据 + 尽可能补齐 `pdf_url` + 部分 PDF 文件样例”的交付策略，更适合在 24 小时约束内稳定完成

如需扩大样例规模，可修改 `config.yaml` 中的 `pdf.max_downloads`

## 配置说明

编辑 `config.yaml`:

```yaml
search:
  query: "analysis"     # 本次采集使用的搜索词
  max_papers: 5500      # 目标论文数
  sort: "date"          # 排序方式

pacing:
  delay_range: [2.0, 4.0]  # 翻页间隔（秒）
  max_retries: 3            # 每页最大重试次数

pdf:
  enabled: true         # 是否下载 PDF
  concurrency: 5        # 并发下载数
  timeout: 60           # 下载超时（秒）
  max_size_mb: 100      # 单文件大小上限
  max_downloads: 200    # 本地下载 PDF 样例数量上限
```

## 性能与效率

| 指标 | 数值 |
|------|------|
| 搜索采集速度 | ~30 条/3秒 = ~600 条/分钟 |
| 5000 条元数据 | ~30 分钟 |
| `pdf_url` 回填 | 取决于外部机构仓储响应速度 |
| PDF 下载 (5并发) | 取决于论文仓库响应速度 |

### 优化方案

1. **搜索阶段**: 在浏览器内用 JS 直接提取 JSON，避免传输完整 HTML
2. **PDF URL 回填阶段**: 对已采集记录异步并发解析公开 PDF 直链，不下载正文文件即可补齐必需字段
3. **PDF 样例下载阶段**: 使用 httpx 异步并发下载，不占用浏览器
4. **断点续爬**: checkpoint 文件记录进度，Ctrl+C 后重启自动续

## 运行监控

- **控制台**: Rich 格式化输出，实时显示进度
- **日志文件**: `logs/crawler_YYYY-MM-DD.log`，自动轮转
- **进度查看**: `python -m src.main --stats`
- **PDF URL 回填**: `python -m src.backfill_pdf_urls`

## 项目结构

```
task1/
├── config.yaml              # 配置文件
├── README.md                # 本文档
├── requirements.txt         # Python 依赖
├── src/
│   ├── main.py              # 入口 + CLI
│   ├── crawler/
│   │   ├── browser.py       # opencli 浏览器桥接
│   │   ├── search.py        # 搜索列表爬取
│   │   └── pdf.py           # PDF 下载
│   ├── parser/
│   │   └── oatd.py          # JS 提取脚本
│   ├── storage/
│   │   └── writer.py        # JSONL/CSV 写入
│   └── utils/
│       ├── checkpoint.py    # 断点续爬
│       └── logger.py        # 日志配置
├── data/
│   ├── metadata/            # 输出: JSONL + CSV
│   ├── pdfs/                # 输出: PDF 文件
│   └── checkpoint.json      # 进度存档
├── logs/                    # 日志
└── src/backfill_pdf_urls.py # 回填 pdf_url
```

## 异常处理

| 场景 | 处理方式 |
|------|---------|
| Cloudflare 拦截 | 使用用户 Chrome 自然通过 |
| 页面加载失败 | 指数退避重试 (最多 3 次) |
| 搜索返回 Oops | 等待 10s 后重试 |
| 某些机构仓储需要校园网/VPN | 保留 `url`，`pdf_url` 置空 |
| 某些详情页需人工点击多步跳转 | 优先尝试自动解析公开直链，失败则保留为空 |
| 仓储迁移/页面失效/临时不可用 | 保留 `url`，便于后续人工复核 |
| PDF 链接失效 | 记录到 checkpoint，继续下一个 |
| Ctrl+C 中断 | 保存 checkpoint，下次自动续 |
| PDF 过大 | 跳过超过 100MB 的文件 |
| 非 PDF 文件 | 校验文件头 (%PDF-)，不合格则删除 |

## 交付口径

- 元数据交付目标：不少于 5,000 条 OATD 论文记录
- 必需字段交付：`title`、`author`、`university`、`year`、`abstract`、`detail_url`、`pdf_url`
- 其中 `pdf_url` 为“如可公开获取则补齐”的直链字段；若源站限制访问，则保留为空并通过 `url` 提供原始论文入口
- 文件交付方式：提供部分已下载 PDF 样例文件，而非将 5,000+ 篇全文全部存盘
