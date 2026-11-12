# NAT gateway EIP traffic metrics

With controller `--enable-metrics=true`, `IptablesEIP` IPv4 traffic is exposed on
the existing kube-ovn-controller `/metrics` endpoint:

```promql
rate(kube_ovn_eip_traffic_bytes_total{eip="my-eip", direction="ingress"}[5m])
rate(kube_ovn_eip_traffic_bytes_total{eip="my-eip", direction="egress"}[5m])
```

The result is bytes per second. Labels are `eip` (CR name), `ip`, `nat_gateway`
and `direction`. The leader synchronizes and reads counters in each gateway every
30 seconds, with at most four concurrent execs and a five-second timeout per gateway. Scrapes
read the collected snapshot; failed collections omit the affected gateway's
samples and log an error instead of reporting zero traffic. Counter creation and
cleanup retry on the next cycle and never block EIP readiness or finalizer release.

Ingress counts IPv4 packets entering the gateway's external interface before
DNAT. Egress counts packets leaving that interface after SNAT, including replies
to DNAT/FIP connections. VPC-side hairpin traffic is excluded. These are IP bytes
at the Netfilter hooks, including packets that might later be dropped, rather
than successfully transmitted wire bytes or application payload bytes.

Counters live in the gateway's network namespace, in the independent nftables
table `ip kube_ovn_eip_metrics`. Each EIP UID owns two named counters. EIP replay,
QoS changes and controller restarts preserve them; the next successful cycle after
EIP deletion removes them, including when a gateway no longer has any EIPs.
A recreated EIP gets fresh counters even when it reuses the same address.
Network namespace or counter recreation resets the values. Collection gaps can
lose the final delta before a reset, so these metrics are for monitoring rather
than lossless billing.

The controller can be upgraded before the NAT gateway image. An old gateway that
does not support the metric command logs collection failures without affecting
EIP creation, deletion or NAT. Once its image is upgraded, the next successful
cycle installs counters for existing EIPs; gateway replacement also restores
counters through this independent cycle. Traffic before counter installation is
not counted. Both gateway Dockerfiles already install nftables; no additional
exporter, RBAC or CRD field is required.

For direct inspection in a gateway:

```sh
bash /kube-ovn/nat-gateway.sh eip-metrics-get
```
