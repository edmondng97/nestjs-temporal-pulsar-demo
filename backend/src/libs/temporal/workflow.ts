// Single import seam for the workflow sandbox. Workflows must only import from
// @temporalio/workflow (no Nest, no Node APIs) — re-exporting here documents that.
export {
  proxyActivities,
  startChild,
  ParentClosePolicy,
} from '@temporalio/workflow';
