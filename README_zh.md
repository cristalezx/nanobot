<div align="center">
  <img src="nanobot_logo.png" alt="nanobot" width="500">
  <h1>nanobot：超轻量级个人 AI 助手</h1>
  <p>
    <a href="https://pypi.org/project/nanobot-ai/"><img src="https://img.shields.io/pypi/v/nanobot-ai" alt="PyPI"></a>
    <a href="https://pepy.tech/project/nanobot-ai"><img src="https://static.pepy.tech/badge/nanobot-ai" alt="Downloads"></a>
    <img src="https://img.shields.io/badge/python-≥3.11-blue" alt="Python">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  </p>
</div>

🐈 **nanobot** 是一款受 [OpenClaw](https://github.com/openclaw/openclaw) 启发的**超轻量级**个人 AI 助手。

⚡️ 用**比 OpenClaw 少 99% 的代码**实现核心 Agent 功能。

📏 实时行数统计：随时运行 `bash core_agent_lines.sh` 验证。

---

## 📢 最新动态

- **2026-02-28** 🚀 发布 **v0.1.4.post3** — 更简洁的上下文、更健壮的会话历史、更智能的 Agent。
- **2026-02-27** 🧠 实验性思考模式支持、钉钉媒体消息、飞书和 QQ 频道修复。
- **2026-02-26** 🛡️ 修复会话注入漏洞、WhatsApp 去重、Windows 路径保护、Mistral 兼容性。

---

## 🌟 核心特性

🪶 **超轻量**：核心 Agent 代码仅约 4,000 行，比同类框架小 99%。

🔬 **科研友好**：代码简洁易读，便于理解、修改和扩展。

⚡️ **极速启动**：最小化占用带来更快启动、更低资源消耗、更快迭代。

💎 **一键部署**：一条命令即可启动，立即可用。

---

## 🏗️ 架构

<p align="center">
  <img src="nanobot_arch.png" alt="nanobot architecture" width="800">
</p>

---

## 📦 安装

**从源码安装**（最新功能，推荐开发使用）

```bash
git clone https://github.com/HKUDS/nanobot.git
cd nanobot
pip install -e .
```

**使用 [uv](https://github.com/astral-sh/uv) 安装**（稳定、快速）

```bash
uv tool install nanobot-ai
```

**从 PyPI 安装**（稳定版）

```bash
pip install nanobot-ai
```

---

## 🚀 快速开始

**1. 初始化**

```bash
nanobot onboard
```

**2. 配置** (`~/.nanobot/config.json`)

设置 API Key（以 OpenRouter 为例，全球用户推荐）：

```json
{
  "providers": {
    "openrouter": {
      "apiKey": "sk-or-v1-xxx"
    }
  }
}
```

设置模型：

```json
{
  "agents": {
    "defaults": {
      "model": "anthropic/claude-opus-4-5",
      "provider": "openrouter"
    }
  }
}
```

**3. 开始聊天**

```bash
nanobot agent
```

2 分钟内即可拥有一个可用的 AI 助手！

---

## 💬 聊天平台集成

| 渠道 | 所需条件 |
|------|---------|
| **Telegram** | @BotFather 生成的 Bot Token |
| **Discord** | Bot Token + Message Content Intent |
| **WhatsApp** | 扫描二维码 |
| **飞书** | App ID + App Secret |
| **钉钉** | AppKey + AppSecret |
| **Slack** | Bot Token + App-Level Token |
| **邮件** | IMAP/SMTP 账号 |
| **QQ** | App ID + App Secret |

---

## ⚙️ 配置参考

配置文件路径：`~/.nanobot/config.json`

### LLM 服务商

| 服务商 | 说明 | 获取 API Key |
|--------|------|-------------|
| `custom` | 任意 OpenAI 兼容接口 | — |
| `openrouter` | 聚合网关（推荐） | [openrouter.ai](https://openrouter.ai) |
| `anthropic` | Claude 直连 | [console.anthropic.com](https://console.anthropic.com) |
| `openai` | GPT 直连 | [platform.openai.com](https://platform.openai.com) |
| `deepseek` | DeepSeek 直连 | [platform.deepseek.com](https://platform.deepseek.com) |
| `siliconflow` | 硅基流动 | [siliconflow.cn](https://siliconflow.cn) |
| `volcengine` | 火山引擎 | [volcengine.com](https://www.volcengine.com) |
| `dashscope` | 通义千问 | [dashscope.console.aliyun.com](https://dashscope.console.aliyun.com) |
| `moonshot` | Moonshot/Kimi | [platform.moonshot.cn](https://platform.moonshot.cn) |
| `zhipu` | 智谱 GLM | [open.bigmodel.cn](https://open.bigmodel.cn) |
| `vllm` | 本地模型（任意 OpenAI 兼容服务器） | — |

### MCP（模型上下文协议）

nanobot 支持 [MCP](https://modelcontextprotocol.io/)，可连接外部工具服务器。

```json
{
  "tools": {
    "mcpServers": {
      "filesystem": {
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/path/to/dir"]
      }
    }
  }
}
```

### 安全设置

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `tools.restrictToWorkspace` | `false` | 为 `true` 时，将所有 Agent 工具限制在工作区目录内 |
| `channels.*.allowFrom` | `[]` | 允许交互的用户 ID 白名单 |

---

## 🖥️ CLI 命令参考

| 命令 | 说明 |
|------|------|
| `nanobot onboard` | 初始化配置和工作区 |
| `nanobot agent -m "..."` | 单次对话 |
| `nanobot agent` | 进入交互式对话模式 |
| `nanobot gateway` | 启动网关（连接聊天平台） |
| `nanobot status` | 查看状态 |

---

## 🐳 Docker 部署

```bash
docker compose run --rm nanobot-cli onboard   # 首次初始化
vim ~/.nanobot/config.json                     # 填写 API Key
docker compose up -d nanobot-gateway           # 启动网关
```

---

## 📁 项目结构

```
nanobot/
├── agent/          # 🧠 核心 Agent 逻辑
│   ├── loop.py     #    Agent 循环（LLM ↔ 工具执行）
│   ├── context.py  #    Prompt 构建
│   ├── memory.py   #    持久化记忆
│   ├── skills.py   #    技能加载器
│   └── tools/      #    内置工具
├── skills/         # 🎯 内置技能（github、天气、tmux...）
├── channels/       # 📱 聊天渠道集成
├── providers/      # 🤖 LLM 服务商
├── config/         # ⚙️ 配置管理
└── cli/            # 🖥️ 命令行入口
```

---

---

# 🏢 企业内网离线使用指南

> **适用场景**：企业内部网络无法访问外网（OpenRouter、Anthropic、OpenAI 等均不可达），但希望在内网部署并使用 nanobot。

---

## 核心思路

nanobot 通过 `custom` 或 `vllm` provider 支持**任意 OpenAI 兼容接口**，只要内网有一个提供标准 `/v1/chat/completions` 接口的 LLM 服务，就可以完全离线运行。

```
内网用户 → nanobot → 内网 LLM 服务（vLLM / Ollama / 其他）
                         ↓
                   （无需外网）
```

---

## 最小尝试：5 分钟跑通内网版 nanobot

### 第一步：准备内网 LLM 服务

选择以下任一方案（只需其中一个）：

**方案 A：Ollama（最简单，适合单机测试）**

```bash
# 安装 Ollama（内网机器）
curl -fsSL https://ollama.com/install.sh | sh
# 如果无法联网安装，从官网下载离线包：https://github.com/ollama/ollama/releases

# 拉取一个小模型（需要能访问到模型文件，或从其他渠道导入）
ollama pull qwen2.5:7b

# 启动服务（默认监听 localhost:11434）
ollama serve
```

Ollama 提供 OpenAI 兼容接口：`http://localhost:11434/v1`

**方案 B：vLLM（适合 GPU 服务器）**

```bash
pip install vllm

# 启动本地模型服务（模型文件需提前下载到内网）
vllm serve /path/to/model --port 8000 --host 0.0.0.0
```

接口地址：`http://your-gpu-server:8000/v1`

**方案 C：已有内网 LLM 网关**

如果企业已部署统一的 LLM 服务（如内网部署的 FastChat、LM Studio Server、企业自建 API 网关等），直接记录其地址和 API Key 即可。

---

### 第二步：安装 nanobot（离线环境）

**有内网 PyPI 镜像时：**

```bash
pip install nanobot-ai -i http://your-internal-pypi/simple/
```

**无镜像，从源码安装：**

```bash
# 在有网络的机器上打包
git clone https://github.com/HKUDS/nanobot.git
pip download -r nanobot/requirements.txt -d ./packages
# 将 nanobot/ 目录和 packages/ 拷贝到内网机器

# 在内网机器上安装
pip install --no-index --find-links=./packages -e ./nanobot
```

---

### 第三步：配置 nanobot 连接内网 LLM

```bash
nanobot onboard
```

编辑 `~/.nanobot/config.json`，使用 `custom` provider 指向内网服务：

```json
{
  "providers": {
    "custom": {
      "apiKey": "no-key-needed",
      "apiBase": "http://localhost:11434/v1"
    }
  },
  "agents": {
    "defaults": {
      "model": "qwen2.5:7b",
      "provider": "custom"
    }
  }
}
```

> - `apiBase`：替换为实际的内网 LLM 服务地址
> - `model`：替换为实际部署的模型名称
> - `apiKey`：本地服务通常不需要，填任意非空字符串即可

---

### 第四步：验证可用

```bash
# 简单测试一句话
nanobot agent -m "你好，请介绍一下你自己"
```

如果能收到模型回复，说明内网配置成功！

---

### 第五步（可选）：CLI 纯本地使用

不需要任何聊天平台（Telegram / 飞书等均需外网），直接使用 CLI 模式即可完全在内网运行：

```bash
# 交互式对话
nanobot agent

# 单次提问
nanobot agent -m "帮我写一个 Python 排序函数"

# 查看纯文本输出（不渲染 Markdown）
nanobot agent --no-markdown -m "列出 3 个优化建议"
```

---

## 内网场景下的实用功能

### 1. 文件处理 / 代码助手

nanobot 内置文件读写工具，可直接操作内网服务器上的文件：

```
nanobot agent -m "读取 /var/log/app.log，分析最近的错误信息并给出修复建议"
```

### 2. 执行本地命令

```
nanobot agent -m "检查当前目录下所有 Python 文件，统计代码行数"
```

### 3. 定时任务（心跳）

编辑 `~/.nanobot/workspace/HEARTBEAT.md`：

```markdown
## 定时任务

- [ ] 每 30 分钟检查 /var/log/error.log 中的新增错误
- [ ] 监控磁盘使用率，超过 80% 时告警
```

启动网关后，nanobot 每 30 分钟自动执行这些任务。

### 4. 内网邮件集成

如果企业有内网邮件服务器（IMAP/SMTP），可配置邮件渠道，无需外网：

```json
{
  "channels": {
    "email": {
      "enabled": true,
      "consentGranted": true,
      "imapHost": "mail.your-company.com",
      "imapPort": 993,
      "imapUsername": "nanobot@your-company.com",
      "imapPassword": "your-password",
      "smtpHost": "smtp.your-company.com",
      "smtpPort": 587,
      "smtpUsername": "nanobot@your-company.com",
      "smtpPassword": "your-password",
      "fromAddress": "nanobot@your-company.com",
      "allowFrom": ["you@your-company.com"]
    }
  }
}
```

### 5. 飞书 / 钉钉（内网可用）

飞书和钉钉使用 **WebSocket 长连接 / Stream 模式**，机器人本身**不需要公网 IP**，只需能访问飞书/钉钉的服务端即可。如果企业内网能访问这两个平台（即使外网受限但这两个 SaaS 平台可达），可直接集成。

---

## 常见问题

**Q：模型回复质量不好怎么办？**

推荐使用参数量较大的模型（≥14B），如 `qwen2.5:14b`、`deepseek-r1:14b` 等。在 GPU 资源充足的服务器上效果更好。

**Q：能使用多实例吗？**

可以。每个实例使用不同工作区和端口：

```bash
nanobot gateway -w ~/.nanobot/botA -p 18791
nanobot gateway -w ~/.nanobot/botB -p 18792
```

**Q：安全加固建议？**

生产环境建议开启工作区限制，防止 Agent 操作工作区外的文件：

```json
{
  "tools": {
    "restrictToWorkspace": true
  }
}
```

---

## 最小尝试任务清单

以下是在企业内网中验证 nanobot 可用性的最小步骤，预计 **30 分钟内**完成：

| 步骤 | 任务 | 验证方法 |
|------|------|---------|
| 1 | 启动内网 Ollama 服务并加载一个模型 | `curl http://localhost:11434/v1/models` 返回模型列表 |
| 2 | 安装 nanobot 并运行 `onboard` | 生成 `~/.nanobot/config.json` |
| 3 | 配置 `custom` provider 指向内网 LLM | 填写 `apiBase` 和 `model` |
| 4 | 运行 `nanobot agent -m "你好"` | 收到模型回复 |
| 5 | （可选）配置内网邮件或飞书渠道 | `nanobot gateway` 启动后渠道正常连接 |
