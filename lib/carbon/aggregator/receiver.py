from carbon.instrumentation import increment
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.rewrite import RewriteRuleManager
from carbon import events
from carbon import log


def process(metric, datapoint):
#  log.setDebugEnabled(True);  
  increment('datapointsReceived')
  print("receiver.py:process(): datapoint=%s\n" % str(datapoint))
  for rule in RewriteRuleManager.preRules:
    print("receiver.py:process(): preRules: before apply:metric=%s\n" % metric)
    metric = rule.apply(metric)
    print("receiver.py:process(): preRules: after apply:metric=%s\n" % str(metric))

  aggregate_metrics = []

  for rule in RuleManager.rules:
    print("receiver.py:process(): rule=%s; metric=%s;\n" % (str(rule), str(metric)))
    aggregate_metric = rule.get_aggregate_metric(metric)
    print("receiver.py:process(): aggregate_metric=%s\n" % str(aggregate_metric))

    if aggregate_metric is None:
      continue
    else:
      aggregate_metrics.append(aggregate_metric)

    buffer = BufferManager.get_buffer(aggregate_metric)

    if not buffer.configured:
      buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

    buffer.input(datapoint)

  for rule in RewriteRuleManager.postRules:
    print("receiver.py:process(): postRules: before apply:metric=%s\n" % str(metric))
    metric = rule.apply(metric)
    print("receiver.py:process(): postRules: after apply:metric=%s\n" % str(metric))

  if metric not in aggregate_metrics:
    print("receiver.py:process(): metric not in aggregate_metrics")
    events.metricGenerated(metric, datapoint)
