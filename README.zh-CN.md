# Magpie（鹊）

[English](README.md) · [简体中文](README.zh-CN.md)

Magpie 是 [Bamboo](https://github.com/bigduu/Bamboo-agent) 的独立即时通讯连接器。它让你在
**飞书 / Lark** 或 Telegram 里驱动 Bamboo agent 会话，只通过 Bamboo 公开的 HTTP/WS API 通信，
从不调用进程内部接口；它以 Bamboo **服务插件**的形式发布：由 bamboo-server 负责安装、启动、
监控和重启。

## 在聊天软件里继续干活

在飞书 / Lark 或 Telegram 里给正在运行的 Bamboo agent 发一条请求，回复会出现在同一个对话里。
如果你想给已有的 Bamboo 加一个即时通讯入口，Magpie 就很合适；它不能替代 agent 运行时，也不
负责模型 provider 的配置。用每个平台的 `allow_from` 列表限制谁可以使用。

你需要：一个可访问的 Bamboo 服务、一份已配对的 Bamboo 设备凭据，以及所选平台的机器人/应用凭据。
Telegram 使用轮询，飞书 / Lark 使用长连接；按本文配置不需要对外暴露 webhook 地址。

## 源码与发布版本

最新发布版本是 [`v0.1.1`](https://github.com/bigduu/Magpie/releases/tag/v0.1.1)。`main` 已经
改为 `0.1.2` 版本号，并包含尚未发布的修复：回答恢复的提问后回复丢失、重连或重启后恢复挂起的提问、
提问挂起期间的消息排队和 `/stop`，以及尚未配置时等待配置而不是反复退出重启。不要假定 `v0.1.1`
二进制里已有这些修复；见[完整对比](https://github.com/bigduu/Magpie/compare/v0.1.1...main)和
[审计记录](./docs/readme-audit.md)。

名字来自鹊桥（_què qiáo_）：七夕传说里横跨银河、连接两个被分隔世界的喜鹊之桥。Magpie 跨越的
是聊天平台和正在运行的 Bamboo agent 之间的距离。

本仓库是 [Bamboo #477](https://github.com/bigduu/Bamboo-agent/issues/477) 的成果：把
bamboo-server 进程内的 `connect/` 模块拆成独立二进制。完整设计、Magpie 依赖的 Bamboo API
以及 `src/` 的结构见 [`ARCHITECTURE.md`](./ARCHITECTURE.md)。

## 安装

[Magpie releases](https://github.com/bigduu/Magpie/releases/latest) 发布两种不同的制品：

- `magpie-plugin-v<version>.tar.gz` 是 Bamboo 服务插件包。在 `bamboo serve` 运行时，用下面的
  命令安装当前版本（`v0.1.1`）：

  ```bash
  bamboo plugin install https://github.com/bigduu/Magpie/releases/download/v0.1.1/magpie-plugin-v0.1.1.tar.gz
  ```

  以后的版本把两处 `0.1.1` 都换成对应的版本号即可。

  Bamboo 会用默认信任库中的 Magpie 公钥校验签名 release 的 `.sig` 文件，不需要任何跳过校验的
  参数。之后由 Bamboo 选择对应平台的二进制并管理服务的生命周期。把 `magpie.json` 放到
  `<Bamboo 数据目录>/plugin_service_config/magpie/config.json`（通常是
  `~/.bamboo/plugin_service_config/magpie/config.json`），Bamboo 会把这个路径传给 Magpie。
- `magpie-v<version>-<target>.*` 是独立二进制。如果由其他进程管理器来管理 Magpie，就用它；也可以
  从源码构建同样的二进制：

  ```bash
  git clone https://github.com/bigduu/Magpie.git
  cd Magpie
  cargo build --release
  ./target/release/magpie --config ./magpie.json --check   # 冒烟测试认证和连通性
  ./target/release/magpie --config ./magpie.json            # 运行
  ```

`--check` 会对配置的 Bamboo 实例调用 `GET /api/v1/execute/defaults`，打印解析出的模型后退出。
接入平台之前，可以用它确认设备 token 和 base URL 是否正确。它不会验证 Telegram/飞书凭据、消息
投递，也不会跑完整的 agent 任务。

## 配置

Magpie 按以下优先级读取 `magpie.json`：`--config` 参数，然后是 `$BAMBOO_PLUGIN_SERVICE_CONFIG`，
最后是 `./magpie.json`。在 Unix 上，如果配置文件对同组或其他用户可读，启动时会发出警告（文件里以
明文保存 bamboo 设备 token 和平台机器人密钥；见 `ARCHITECTURE.md`）。

```json
{
  "bamboo": {
    "base_url": "http://127.0.0.1:9562",
    "device_id": "bamboo_abc123",
    "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  },
  "platforms": [
    { "type": "telegram", "token": "123456:ABC-DEF...", "allow_from": ["123456789"] },
    {
      "type": "feishu",
      "app_id": "cli_xxxxxxxx",
      "app_secret": "xxxxxxxxxxxxxxxx",
      "domain": "feishu",
      "allow_from": ["ou_xxxxxxxx"]
    }
  ]
}
```

`bamboo.device_id`/`bamboo.token` 是 Bamboo 签发的已配对设备凭据，Magpie 的每个请求都以这个
设备的身份认证。Lotus Next 目前还没有设备配对界面，需要调用 Bamboo 的 `POST /v2/pair` 接口获取。
在 Bamboo 已启用访问密码的情况下：

```bash
curl -sS -X POST http://127.0.0.1:9562/v2/pair \
  -H 'Content-Type: application/json' \
  -d '{"root_password":"<你的 Bamboo 访问密码>","label":"magpie"}'
```

把返回的 `device_id` 填入 `bamboo.device_id`，`device_token` 填入 `bamboo.token`；token 只显示
一次。也可以由一台已配对的设备通过 `POST /v2/pair/code` 申请一次性配对码，再用
`{"code":"<配对码>","label":"magpie"}` 兑换。见 Bamboo 的
[配对设计](https://github.com/bigduu/Bamboo-agent/blob/dev/docs/design/api-v2-transport.md)。
`allow_from` 为空时会拒绝所有入站消息（启动时记录警告），因此刚配置好的平台条目不会意外地对整个
互联网开放。

完整的配置 schema、Bamboo API 对照表，以及从 bamboo 进程内 `connect/` 模块继承的约束，见
[`ARCHITECTURE.md`](./ARCHITECTURE.md)。

在 Unix 上，请运行 `chmod 600 magpie.json` 让本地配置保持私有。配置所在目录也要保护好：Magpie
会把对话/会话映射保存在配置文件旁边。插件构建面向 macOS、Linux 和 Windows；本次文档审查没有实测
各平台的原生行为，也没有通过真实即时通讯账号收发消息。

## 飞书 / Lark 配置

以下步骤对应 `src/platforms/feishu/` 中飞书适配器实际实现的内容：

1. 在[飞书开放平台](https://open.feishu.cn/app)（或 [Lark Developer](https://open.larksuite.com/app)）
   创建一个企业自建应用，并开启**机器人**能力。把 App ID（`cli_…`）和 App Secret 分别填入
   `app_id` / `app_secret`。
2. 在事件订阅中选择**长连接**方式接收事件（不需要公网地址），并订阅 `im.message.receive_v1`。
   给应用开通接收消息、以机器人身份发送消息所需的消息权限。
3. 如果想用按钮回答 agent 的提问和权限确认，还要通过长连接开启卡片回调 `card.action.trigger`。
   直接打字回复也可以。
4. 飞书（国内）设置 `"domain": "feishu"`，Lark 设置 `"domain": "lark"`；也可以填一个 `https://`
   开头的 base URL。
5. 把你的 `open_id`（`ou_…`）加进 `allow_from`。如果不知道自己的 open_id，先启动 Magpie，给机器人
   发一条消息，然后在日志里找 `rejected inbound message — user not in allow_from` 这条警告，里面
   记录了你的 `user_id`。
6. 发布应用版本，让它在你的组织内可用，然后直接私聊机器人。在群聊里，只有 @ 机器人时 Magpie 才会
   响应。

聊天命令：`/new` 开始新会话，`/stop` 停止当前任务，`/status` 查看当前会话。回复以文本卡片形式
发送，不会发送图片和文件。

上面提到的控制台菜单和权限名称在飞书与 Lark 的不同版本中可能略有差异；本次审查没有创建真实应用。
