// Static, hand-curated snippets + rationale. No network — everything inlined.
window.NODES = [
  {
    id: 'execute',
    title: 'executeCampaignWorkflow (父 Workflow)',
    what: 'HTTP 触发后由 Temporal 调度。先跑 targeting 生成受众，再 startChild 派发子 workflow。',
    why: 'Temporal 提供持久化执行与自动重试：进程崩溃后 workflow 从上次状态续跑。ParentClosePolicy.ABANDON 让父完成后子继续运行。',
    code: `export async function executeCampaignWorkflow(input) {
  await targeting({ campaignId: input.campaignId });
  await startChild(CAMPAIGN_DISPATCHER_WORKFLOW_TYPE, {
    parentClosePolicy: ParentClosePolicy.ABANDON,
    args: [{ campaignId: input.campaignId, epoch: 0 }],
  });
}`,
  },
  {
    id: 'dispatch',
    title: 'dispatchPlayers (Activity)',
    what: '分页拉 PENDING delivery，逐行 CAS PENDING→IN_PROGRESS，命中才发 Pulsar 消息（带 epoch）。',
    why: 'Mark-first CAS 保证幂等：Temporal 重试不会重复发送。每页 heartbeat 让长任务可被监控与续跑。',
    code: `const cas = await markInProgressIfPending(delivery._id);
if (cas.modifiedCount !== 1) return;   // 已被并发/暂停占用，跳过
await producer.send({ deliveryId, campaignId, epoch });`,
  },
  {
    id: 'pulsar',
    title: 'Pulsar (Shared 订阅)',
    what: 'dispatchPlayers 作为 producer 投递，多个 consumer 以 Shared 订阅并行消费同一 backlog。',
    why: 'Shared 订阅水平扩展消费吞吐；ackTimeout + 不 ack 即重投，保证 at-least-once，配合下游 CAS 去重达成 effectively-once。',
    code: `subscriptionType: 'Shared',
ackTimeoutMs: 5 * 60 * 1000,
// 未 ack → Pulsar 超时后重投，绝不静默丢消息`,
  },
  {
    id: 'consumer',
    title: 'CampaignDeliveryConsumer',
    what: '重读 delivery → epoch 栅栏 → CAS IN_PROGRESS→SENDING → 空投递 → 写终态。',
    why: '消息只是触发器，真值源是 DB 行。epoch 栅栏丢弃暂停/rewind 前的陈旧消息，CAS 防并发重复处理。',
    code: `if (data.epoch < currentEpoch) return;          // epoch 栅栏
const cas = await markSendingIfInProgress(id);
if (cas.modifiedCount !== 1) return;            // 并发输者跳过
await deliverStub();
await markTerminal({ status: ok ? 'SUCCESS' : 'FAILED' });`,
  },
  {
    id: 'resume',
    title: 'Pause / Resume (epoch rewind)',
    what: 'pause 写 Redis 标志让 dispatcher 早停；resume 清标志 + epoch++ + 把 IN_PROGRESS 回退为 PENDING。',
    why: 'epoch++ 让暂停前在途的旧消息全部被 consumer 栅栏丢弃；rewind 把"已标记但消息被栅栏丢弃"的孤儿行重新放回 PENDING，让新一轮以新 epoch 重投——既不重复投递也不丢行。',
    code: `await redis.setPaused(id, false);
const epoch = await campaign.bumpEpoch(id);       // 旧消息全部失效
await delivery.rewindInProgressToPending(id);     // 孤儿行回到 PENDING
await temporal.start(EXECUTE_CAMPAIGN_WORKFLOW_TYPE, { args:[{campaignId:id}] });`,
  },
  {
    id: 'reconcile',
    title: 'Reconciliation Cron',
    what: '定时扫描 IN_PROGRESS campaign，当无非终态 delivery 时标记 COMPLETED。',
    why: '完成判定不放在 dispatcher（它可能因 ABANDON/崩溃提前退出），由独立 cron 兜底 → 最终一致性。',
    code: `if (nonTerminal === 0) {
  await campaignService.markCompleted(c._id);
}`,
  },
];

window.STATES = ['PENDING', 'IN_PROGRESS', 'SENDING', 'SUCCESS', 'FAILED'];
