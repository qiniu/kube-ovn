//go:build linux

package speaker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cilium/ebpf"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

type tcNetlinkOps interface {
	QdiscAdd(netlink.Qdisc) error
	FilterList(netlink.Link, uint32) ([]netlink.Filter, error)
	FilterReplace(netlink.Filter) error
	FilterDel(netlink.Filter) error
	LinkByName(string) (netlink.Link, error)
	LinkByIndex(int) (netlink.Link, error)
	LinkList() ([]netlink.Link, error)
}

type defaultTCNetlinkOps struct{}

func (defaultTCNetlinkOps) QdiscAdd(q netlink.Qdisc) error {
	return netlink.QdiscAdd(q)
}

func (defaultTCNetlinkOps) FilterList(link netlink.Link, parent uint32) ([]netlink.Filter, error) {
	return netlink.FilterList(link, parent)
}

func (defaultTCNetlinkOps) FilterReplace(f netlink.Filter) error {
	return netlink.FilterReplace(f)
}

func (defaultTCNetlinkOps) FilterDel(f netlink.Filter) error {
	return netlink.FilterDel(f)
}

func (defaultTCNetlinkOps) LinkByName(name string) (netlink.Link, error) {
	return netlink.LinkByName(name)
}

func (defaultTCNetlinkOps) LinkByIndex(index int) (netlink.Link, error) {
	return netlink.LinkByIndex(index)
}

func (defaultTCNetlinkOps) LinkList() ([]netlink.Link, error) {
	return netlink.LinkList()
}

var netlinkOps tcNetlinkOps = defaultTCNetlinkOps{}

// ensureClsactQdisc 确保指定网卡已附加 clsact qdisc（若已存在则忽略 EEXIST）。
func ensureClsactQdisc(link netlink.Link) error {
	qdisc := &netlink.GenericQdisc{QdiscAttrs: netlink.QdiscAttrs{
		LinkIndex: link.Attrs().Index, Handle: netlink.MakeHandle(0xffff, 0), Parent: netlink.HANDLE_CLSACT,
	}, QdiscType: "clsact"}
	if err := netlinkOps.QdiscAdd(qdisc); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}

// tcBPFProgram 抽象 BPF Program 的基本接口，便于纯单元测试 mock 并统一获取 Program 关键元数据。
type tcBPFProgram interface {
	FD() int
	ID() (uint32, bool)
	Tag() string
}

// ebpfTCProgram 包装 *ebpf.Program 以适配 tcBPFProgram 接口，统一 ProgInfo 的提取与观测日志。
type ebpfTCProgram struct {
	p *ebpf.Program
}

func newEbpfTCProgram(p *ebpf.Program) tcBPFProgram {
	if p == nil {
		return nil
	}
	return &ebpfTCProgram{p: p}
}

func (e *ebpfTCProgram) FD() int {
	return e.p.FD()
}

func (e *ebpfTCProgram) ID() (uint32, bool) {
	info, err := e.p.Info()
	if err != nil {
		klog.Warningf("failed to get BPF program info: %v", err)
		return 0, false
	}
	id, ok := info.ID()
	if !ok {
		klog.V(4).Infof("BPF program info has no kernel ID yet")
	}
	return uint32(id), ok
}

func (e *ebpfTCProgram) Tag() string {
	info, err := e.p.Info()
	if err != nil {
		return ""
	}
	return info.Tag
}

// tcFilterConfig 封装通用的 TC BPF Ingress 过滤器挂载参数。
type tcFilterConfig struct {
	Link netlink.Link
	// Program 指向待挂载的 eBPF 程序对象
	Program tcBPFProgram
	// AttachName 为本次挂载与更新使用的正式 Filter 规则名
	AttachName string
	// LegacyNames 为待兼容匹配并剥离的历史遗留规则名列表
	LegacyNames []string
	Priority    uint16
}

// attachBPFFilter 在指定网卡的 Ingress 队列上原子挂载或就地替换（Atomic Swap）eBPF TC 过滤器。
// 1. 自动确保网卡具有 clsact qdisc。
// 2. 检查目标 priority 是否可用，并收集其他 priority 上的旧规则。
// 3. 若当前规则尚未就绪，使用 netlink.FilterReplace 执行原子挂载或替换。
// 4. 新规则就绪后，再清理其他 priority 上的旧规则。
func attachBPFFilter(cfg tcFilterConfig) error {
	if cfg.Link == nil {
		return errors.New("link is nil")
	}
	if cfg.Link.Attrs() == nil {
		return errors.New("link attrs is nil")
	}
	if cfg.Program == nil {
		return errors.New("bpf program is nil")
	}
	if cfg.AttachName == "" {
		return errors.New("AttachName must not be empty")
	}
	progFD := cfg.Program.FD()
	progID, _ := cfg.Program.ID()
	progTag := cfg.Program.Tag()
	if err := ensureClsactQdisc(cfg.Link); err != nil {
		return err
	}
	filters, err := netlinkOps.FilterList(cfg.Link, netlink.HANDLE_MIN_INGRESS)
	if err != nil {
		return err
	}
	allNames := append([]string{cfg.AttachName}, cfg.LegacyNames...)
	linkName := cfg.Link.Attrs().Name
	var matchedCurrent bool
	var obsolete []netlink.Filter
	for _, filter := range filters {
		attrs := filter.Attrs()
		if attrs == nil {
			continue
		}
		bpf, ok := filter.(*netlink.BpfFilter)
		if attrs.Priority != cfg.Priority {
			// 先收集旧规则，待目标 priority 校验并挂载成功后再清理，避免迁移失败破坏存量转发（Make-Before-Break）。
			// 挂载新规则与删除旧规则之间的短暂共存窗口完全无害：TC Ingress 按 Priority 从小到大（数字越小优先级越高）匹配，
			// 两者均执行相同的 EIP 直通转发逻辑并共享同一 BPF Map，数据面保持 0 丢包且不会重复转发。
			if ok && isMatchingTCFilter(bpf, progID, progTag, allNames...) {
				obsolete = append(obsolete, filter)
			}
			continue
		}
		if !ok || !isMatchingTCFilter(bpf, progID, progTag, allNames...) {
			return fmt.Errorf("TC priority %d on %s is already owned by another filter", cfg.Priority, linkName)
		}
		// 严禁在此直接 return nil：否则当同 priority 规则排在前面时，排在后面的异 priority 废弃规则会被跳过清理。
		if isCurrentTCFilter(bpf, progID, cfg.AttachName) {
			matchedCurrent = true
		}
	}
	if !matchedCurrent {
		if err := netlinkOps.FilterReplace(&netlink.BpfFilter{
			FilterAttrs: netlink.FilterAttrs{
				LinkIndex: cfg.Link.Attrs().Index, Parent: netlink.HANDLE_MIN_INGRESS,
				Handle: 1, Priority: cfg.Priority, Protocol: unix.ETH_P_ALL,
			},
			Fd: progFD, Name: cfg.AttachName, DirectAction: true,
		}); err != nil {
			return err
		}
	}
	for _, filter := range obsolete {
		if err := netlinkOps.FilterDel(filter); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENODEV) && !errors.Is(err, unix.ESRCH) {
			klog.Warningf("failed to remove obsolete TC filter at priority %d on %s: %v", filter.Attrs().Priority, linkName, err)
		}
	}
	return nil
}

// isCurrentTCFilter 校验规则是否已是当前 Program 在目标 Priority 上的正式生效规则。
func isCurrentTCFilter(bpf *netlink.BpfFilter, progID uint32, attachName string) bool {
	return bpf != nil && bpf.Name == attachName && progID > 0 && bpf.Id == int(progID) && bpf.DirectAction
}

// isMatchingTCFilter 基于 Ingress parent、Handle=1、Protocol=ALL、DirectAction
// 以及 BPF Program ID / BPF 指令 Tag / 专属名称等多维特征进行精准交叉核验，
// 绝不误删与其他 CNI (如 Cilium/Calico/QoS) 共享的规则。
func isMatchingTCFilter(filter *netlink.BpfFilter, progID uint32, progTag string, filterNames ...string) bool {
	if filter == nil {
		return false
	}
	attrs := filter.Attrs()
	if attrs == nil || attrs.Parent != netlink.HANDLE_MIN_INGRESS || attrs.Handle != 1 || attrs.Protocol != unix.ETH_P_ALL {
		return false
	}
	if !filter.DirectAction {
		return false
	}
	// 强指纹 1：若当前加载了 BPF 实例，且程序运行时内核 ID 相同
	if progID > 0 && filter.Id == int(progID) {
		return true
	}
	// 强指纹 2：若 BPF 字节码指令 SHA Tag 匹配当前编译产物
	if progTag != "" && filter.Tag != "" && strings.EqualFold(filter.Tag, progTag) {
		return true
	}
	// 强指纹 3：专属 Filter 唯一名称标识
	for _, name := range filterNames {
		if name != "" && filter.Name == name {
			return true
		}
	}
	return false
}

// detachBPFFiltersOnLinks 安全遍历指定网卡（为空则遍历所有网卡）并按 matchFunc 卸载符合条件的 TC Filter。
// 对无 clsact qdisc（ENOENT）或已被并发删除的虚拟网卡（ENODEV），静默视为已清理。
func detachBPFFiltersOnLinks(linkName string, matchFunc func(*netlink.BpfFilter) bool) error {
	var links []netlink.Link
	if linkName != "" {
		link, err := netlinkOps.LinkByName(linkName)
		if err != nil {
			var linkNotFound netlink.LinkNotFoundError
			if errors.As(err, &linkNotFound) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.ENOENT) {
				return nil
			}
			return err
		}
		links = []netlink.Link{link}
	} else {
		var err error
		links, err = netlinkOps.LinkList()
		if err != nil {
			return err
		}
	}
	var errs []error
	for _, link := range links {
		if link == nil || link.Attrs() == nil {
			continue
		}
		filters, err := netlinkOps.FilterList(link, netlink.HANDLE_MIN_INGRESS)
		if err != nil {
			if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENODEV) {
				klog.V(4).Infof("interface %s has no clsact qdisc or is gone, skipping filter detach: %v", link.Attrs().Name, err)
				continue
			}
			errs = append(errs, err)
			continue
		}
		for _, filter := range filters {
			if bpf, ok := filter.(*netlink.BpfFilter); ok && matchFunc(bpf) {
				if err := netlinkOps.FilterDel(filter); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENODEV) && !errors.Is(err, unix.ESRCH) {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}
