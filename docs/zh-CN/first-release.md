# 首版配置基线

当前尚未首次发布，不支持开发候选之间的原地升级、旧包名兼容或自动控制模式迁移。内部格式编号不等于已发布版本。

首次部署前一次确定所有节点的控制库、访问模式、角色、密钥和持久目录。完整管理恢复能力使用一致的配置：

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
  recoveryCommit: true
  recoverySkip: true
```

命名空间创建时直接写入对应模式；不匹配立即拒绝，停止全部节点也不会自动迁移。已有开发环境需要另行评估并建立新的测试基线，不得把删除代码历史理解成删除业务数据或只复制游标。

Linux和Docker使用同一份角色配置及每节点独立目录。Helm使用控制器/Agent values叠加`values-recovery-skip.yaml`；手工Kubernetes可参考`rivomesh-skip-*.yaml`。示例不证明真实K8s/CSI已通过验收。

任务/会话/证据/回执的重启恢复、租约与epoch隔离、权限撤销和损坏/旧备份拒绝仍是当前功能，不属于应删除的开发版本兼容代码。

V2/V3双执行链还在单独合并中。以[当前状态](implementation-status.md)与[SDD](../sdd/001-control-recovery-compatibility/spec.md)为准，不把清理标成其他未完成功能的完成。
