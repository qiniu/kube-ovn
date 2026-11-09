//go:build linux

package speaker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

type tcNetlinkOps interface {
	QdiscAdd(netlink.Qdisc) error
	FilterList(netlink.Link, uint32) ([]netlink.Filter, error)
	FilterReplace(netlink.Filter) error
	FilterDel(netlink.Filter) error
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

// tcBPFProgram 抽象 BPF Program 的基本接口，便于纯单元测试 mock。
type tcBPFProgram interface {
	FD() int
	Info() (*ebpf.ProgramInfo, error)
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
// 2. 检查现有过滤器，若已有相同 ID 的 direct-action 规则已就绪，直接幂等返回。
// 3. 自动探测并清理本程序历史遗留在其他 priority 上的旧规则。
// 4. 使用 netlink.FilterReplace 执行零中断无锁 RCU 热更。
func attachBPFFilter(cfg tcFilterConfig) error {
	if cfg.Link == nil {
		return errors.New("link is nil")
	}
	if cfg.Program == nil {
		return errors.New("bpf program is nil")
	}
	if cfg.AttachName == "" {
		return errors.New("AttachName must not be empty")
	}
	progFD := cfg.Program.FD()
	var progID uint32
	var progTag string
	if info, err := cfg.Program.Info(); err != nil {
		klog.Warningf("failed to get BPF program info: %v", err)
	} else if info != nil {
		if id, ok := info.ID(); ok {
			progID = uint32(id)
		} else {
			klog.V(4).Infof("BPF program info has no kernel ID yet")
		}
		progTag = info.Tag
	}
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
	for _, filter := range filters {
		attrs := filter.Attrs()
		if attrs == nil {
			continue
		}
		bpf, ok := filter.(*netlink.BpfFilter)
		if attrs.Priority != cfg.Priority {
			// 若发现本程序历史遗留在其他 priority 上的旧规则（如升级修改了 priority 配置），主动剥离避免旧规则拦截或双规则共存
			if ok && isMatchingTCFilter(bpf, progID, progTag, allNames...) {
				if err := netlinkOps.FilterDel(filter); err != nil && !errors.Is(err, unix.ENOENT) {
					klog.Warningf("failed to remove obsolete TC filter at priority %d on %s: %v", attrs.Priority, linkName, err)
				}
			}
			continue
		}
		if !ok || !isMatchingTCFilter(bpf, progID, progTag, allNames...) {
			return fmt.Errorf("TC priority %d on %s is already owned by another filter", cfg.Priority, linkName)
		}
		// 严禁在此直接 return nil：否则当同 priority 规则排在前面时，排在后面的异 priority 废弃规则会被跳过清理。
		if bpf.Id == int(progID) && bpf.DirectAction {
			matchedCurrent = true
		}
	}
	if matchedCurrent {
		return nil
	}
	return netlinkOps.FilterReplace(&netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			LinkIndex: cfg.Link.Attrs().Index, Parent: netlink.HANDLE_MIN_INGRESS,
			Handle: 1, Priority: cfg.Priority, Protocol: unix.ETH_P_ALL,
		},
		Fd: progFD, Name: cfg.AttachName, DirectAction: true,
	})
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
		link, err := netlink.LinkByName(linkName)
		if err != nil {
			var linkNotFound netlink.LinkNotFoundError
			if errors.As(err, &linkNotFound) || errors.Is(err, unix.ENODEV) {
				return nil
			}
			return err
		}
		links = []netlink.Link{link}
	} else {
		var err error
		links, err = netlink.LinkList()
		if err != nil {
			return err
		}
	}
	var errs []error
	for _, link := range links {
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
				if err := netlinkOps.FilterDel(filter); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENODEV) {
					errs = append(errs, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

// pruneBPFHashMap 安全清理 BPF Hash Map 中的过期条目。
// 必须在删除前先完整收集过期 Key：直接在遍历游标处删除可能导致 BPF Hash Map 遍历重置，当保留部分条目时可能发生死循环。
func pruneBPFHashMap(targetMap *ebpf.Map, wanted map[string]bool) error {
	var stale [][]byte
	var key any
	for {
		next, err := targetMap.NextKeyBytes(key)
		if err != nil {
			// 兼容不同 ebpf/内核版本在迭代到达结尾时返回 ErrKeyNotExist 而非 (nil, nil) 的边界情况
			if errors.Is(err, ebpf.ErrKeyNotExist) {
				break
			}
			return err
		}
		if next == nil {
			break
		}
		if !wanted[string(next)] {
			stale = append(stale, next)
		}
		key = next
	}
	var errs []error
	for _, key := range stale {
		if err := targetMap.Delete(key); err != nil && !errors.Is(err, ebpf.ErrKeyNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// removeBPFPin 安全清理 Pinned Map 文件。
func removeBPFPin(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// loadCollectionWithPinnedMaps 加载 BPF CollectionSpec，并优先复用 bpffs 中既有的 Pinned Map，
// 实现进程重启/热更新期间数据面 0 丢包；若复用失败则安全回退，绝不破坏性提前删除旧 Pin 文件，
// 仅在新 Collection 创建成功后原子替换 Pin 路径，确保旧实例回滚能力不受损。
func loadCollectionWithPinnedMaps(spec *ebpf.CollectionSpec, pinMapPaths map[string]string) (*ebpf.Collection, error) {
	for _, ms := range spec.Maps {
		ms.Pinning = ebpf.PinNone
	}
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("remove BPF memlock limit: %w", err)
	}

	replacements := make(map[string]*ebpf.Map)
	// 根据 cilium/ebpf 契约：CollectionOptions.MapReplacements 内部仅克隆 (Clone) 传入的 Map 句柄，
	// 调用方仍然拥有 LoadPinnedMap 打开的原始句柄的所有权，必须显式 Close()，否则每次加载均会导致 FD 泄漏。
	defer func() {
		for _, r := range replacements {
			_ = r.Close()
		}
	}()

	for name, p := range pinMapPaths {
		if pinned, err := ebpf.LoadPinnedMap(p, nil); err == nil {
			replacements[name] = pinned
		}
	}

	coll, err := ebpf.NewCollectionWithOptions(spec, ebpf.CollectionOptions{MapReplacements: replacements})
	if err != nil && len(replacements) > 0 {
		klog.Warningf("failed to load BPF collection with existing pinned maps, falling back to new maps: %v", err)
		// 关键点：绝不在这里提前 removeBPFPin(p)，避免新 Collection 加载失败退出时旧 Pin 被破坏导致无法回滚
		coll, err = ebpf.NewCollection(spec)
	}
	if err != nil {
		return nil, fmt.Errorf("load BPF collection: %w", err)
	}

	// 两阶段安全提交（Two-Phase Commit）：
	// 针对新创建（未被成功复用）的 Map，先全部 Pin 到同目录下的 .tmp 路径；
	// 待所有未复用 Map 均成功创建临时 Pin 后，再统一通过 bpffs 原生支持的 rename(2) 原子替换到目标路径。
	// 若任何环节出错，立即统一清理全部生成的 .tmp 文件并关闭 Collection，彻底杜绝数据面“半边新、半边旧”的分裂态。
	type pendingPin struct {
		name    string
		tmpPath string
		dstPath string
	}
	var pending []pendingPin
	cleanupTmp := func() {
		for _, p := range pending {
			_ = removeBPFPin(p.tmpPath)
		}
	}

	// Phase 1: 将所有未复用的 Map Pin 到 .tmp 临时文件
	for name, path := range pinMapPaths {
		if _, ok := replacements[name]; ok {
			continue
		}
		if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			cleanupTmp()
			coll.Close()
			return nil, fmt.Errorf("create map dir %s: %w", filepath.Dir(path), err)
		}
		tmpPath := path + ".tmp"
		_ = removeBPFPin(tmpPath)
		if err = coll.Maps[name].Pin(tmpPath); err != nil {
			cleanupTmp()
			coll.Close()
			return nil, fmt.Errorf("pin BPF map %s to tmp %s: %w", name, tmpPath, err)
		}
		pending = append(pending, pendingPin{name: name, tmpPath: tmpPath, dstPath: path})
	}

	// Phase 2: 全部 Pin 成功，统一原子重命名覆盖目标路径。
	// 设计妥协说明：
	// 若 Phase 2 执行过程中出现极端罕见故障（如前一个 Map 重命名成功但后续 Map 失败），
	// 已成功覆盖的目标路径无法回滚至旧 Pin 文件。鉴于所有待重命名文件均位于同一 bpffs 挂载点内且权限目录均已验证就绪，
	// 内核 VFS rename(2) 操作在同文件系统内失败概率极低，此两阶段设计可在极小复杂度下杜绝绝大多数中间态。
	for _, p := range pending {
		if err = os.Rename(p.tmpPath, p.dstPath); err != nil {
			cleanupTmp()
			coll.Close()
			return nil, fmt.Errorf("atomic swap pinned map %s: %w", p.dstPath, err)
		}
	}

	return coll, nil
}
