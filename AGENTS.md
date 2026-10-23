# QQ空间开发约定

- 插件显示名称为 `QQ空间`，ID 为 `astrbot_plugin_qzone_life`；一个机器人 QQ 对应一个固定角色。
- 优先复用现有插件：Life Scheduler 提供日程，Gitee AI Image 生成自拍，LivingMemory 保存记忆。不修改它们的源码或私自扩大记忆共享范围。
- QQ空间协议来自用户提供的 `Zhalslar/astrbot_plugin_qzone` v3.1.3；维护来源说明及 GPL-3.0 许可证。
- 自主每天 1—2 条，手动要求不计额度；删除不返还自主额度。只自动回复 AstrBot `admins_id` 的新评论，逐条检查实际作者。
- 发送结果不明时停止自动重发，待管理员核对；记忆同步失败只重试同步。不能把本地状态标记当成真实空间操作成功。
- 功能改动后运行 `python -m unittest discover -s tests -v` 与 `python -m ruff check .`。交付前由另一位智能体交叉审查，修复实质问题再发布。
- 区分本地模拟验证、真实 QQ/AstrBot 联调和市场审核，不将前者描述为后两者。
- 版本号同步 `metadata.yaml`、README、CHANGELOG 与状态信息；发布说明只写实际变化。GitHub 账号为 `chouoi344`，保持本仓库独立，不移动语音插件仓库。
- 数据库、Cookie、运行配置、图片和密钥不得进入 Git 或安装包。维护资料及测试环境保存在源码目录之外。
