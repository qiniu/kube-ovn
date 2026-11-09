//go:build linux

package speaker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cilium/ebpf"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

func TestIsMatchingTCFilter(t *testing.T) {
	baseFilter := func() *netlink.BpfFilter {
		return &netlink.BpfFilter{
			FilterAttrs: netlink.FilterAttrs{
				Handle:   1,
				Parent:   netlink.HANDLE_MIN_INGRESS,
				Protocol: unix.ETH_P_ALL,
				Priority: 5,
			},
			DirectAction: true,
			Name:         "test-tc-filter",
		}
	}

	// 1. nil filter
	require.False(t, isMatchingTCFilter(nil, 0, ""))

	// 2. 名字完全匹配
	f := baseFilter()
	require.True(t, isMatchingTCFilter(f, 0, "", "test-tc-filter"))

	// 3. 名字匹配多候选之一
	require.True(t, isMatchingTCFilter(f, 0, "", "other-filter", "test-tc-filter"))

	// 4. 强指纹：ProgID 匹配
	fID := baseFilter()
	fID.Name = ""
	fID.Id = 99
	require.True(t, isMatchingTCFilter(fID, 99, ""))

	// 5. 强指纹：ProgTag 匹配（忽略大小写）
	fTag := baseFilter()
	fTag.Name = ""
	fTag.Tag = "08218004CAF6A189"
	require.True(t, isMatchingTCFilter(fTag, 0, "08218004caf6a189"))

	// 6. Parent 不为 Ingress 拒绝
	fEgress := baseFilter()
	fEgress.Parent = netlink.HANDLE_MIN_EGRESS
	require.False(t, isMatchingTCFilter(fEgress, 0, "", "test-tc-filter"))

	// 7. DirectAction 为 false 拒绝
	fNoDA := baseFilter()
	fNoDA.DirectAction = false
	require.False(t, isMatchingTCFilter(fNoDA, 0, "", "test-tc-filter"))

	// 8. Handle != 1 拒绝
	fDiffHandle := baseFilter()
	fDiffHandle.Handle = 2
	require.False(t, isMatchingTCFilter(fDiffHandle, 0, "", "test-tc-filter"))

	// 9. 不匹配任何特征拒绝
	fAlien := baseFilter()
	fAlien.Name = "cilium-netdev"
	require.False(t, isMatchingTCFilter(fAlien, 0, "", "test-tc-filter"))
}

func TestRemoveBPFPin(t *testing.T) {
	// 1. 删除不存在的文件应安全返回 nil
	require.NoError(t, removeBPFPin("/tmp/nonexistent-pin-file-12345"))

	// 2. 正常删除已有文件
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test-pin")
	require.NoError(t, os.WriteFile(filePath, []byte("test"), 0o600))
	require.FileExists(t, filePath)
	require.NoError(t, removeBPFPin(filePath))
	require.NoFileExists(t, filePath)
}

func TestAttachBPFFilterNilLink(t *testing.T) {
	err := attachBPFFilter(tcFilterConfig{Link: nil})
	require.ErrorContains(t, err, "link is nil")
}

func TestAttachBPFFilterNilProgram(t *testing.T) {
	err := attachBPFFilter(tcFilterConfig{Link: &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "dummy0"}}})
	require.ErrorContains(t, err, "bpf program is nil")
}

func TestAttachBPFFilterEmptyAttachName(t *testing.T) {
	err := attachBPFFilter(tcFilterConfig{
		Link:    &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "dummy0"}},
		Program: &mockBPFProgram{fd: 1},
	})
	require.ErrorContains(t, err, "AttachName must not be empty")
}

type mockTCNetlinkOps struct {
	qdiscAddErr       error
	filterListFilters []netlink.Filter
	filterListErr     error
	filterReplaceErr  error
	filterDelErr      error

	replacedFilters []netlink.Filter
	deletedFilters  []netlink.Filter
}

func (m *mockTCNetlinkOps) QdiscAdd(netlink.Qdisc) error {
	return m.qdiscAddErr
}

func (m *mockTCNetlinkOps) FilterList(netlink.Link, uint32) ([]netlink.Filter, error) {
	return m.filterListFilters, m.filterListErr
}

func (m *mockTCNetlinkOps) FilterReplace(f netlink.Filter) error {
	m.replacedFilters = append(m.replacedFilters, f)
	return m.filterReplaceErr
}

func (m *mockTCNetlinkOps) FilterDel(f netlink.Filter) error {
	m.deletedFilters = append(m.deletedFilters, f)
	return m.filterDelErr
}

type mockBPFProgram struct {
	fd      int
	info    *ebpf.ProgramInfo
	infoErr error
}

func (m *mockBPFProgram) FD() int {
	return m.fd
}

func (m *mockBPFProgram) Info() (*ebpf.ProgramInfo, error) {
	return m.info, m.infoErr
}

func TestAttachBPFFilterOpsBranches(t *testing.T) {
	origOps := netlinkOps
	defer func() { netlinkOps = origOps }()

	link := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Index: 10, Name: "dummy0"}}
	prog := &mockBPFProgram{fd: 42}

	// 1. 幂等短路分支：同 priority 上已存在匹配本程序的规则
	mock := &mockTCNetlinkOps{
		filterListFilters: []netlink.Filter{
			&netlink.BpfFilter{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: 10, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 5, Protocol: unix.ETH_P_ALL,
				},
				DirectAction: true,
				Name:         "kube-ovn-eip",
			},
		},
	}
	netlinkOps = mock
	err := attachBPFFilter(tcFilterConfig{
		Link:        link,
		Program:     prog,
		AttachName:  "kube-ovn-eip",
		LegacyNames: []string{"tc_eip.o:[classifier]"},
		Priority:    5,
	})
	require.NoError(t, err)
	require.Empty(t, mock.replacedFilters)
	require.Empty(t, mock.deletedFilters)

	// 2. 异 priority 剥离分支：旧 priority 7 存在旧名称规则，新挂载在 priority 5
	mock = &mockTCNetlinkOps{
		filterListFilters: []netlink.Filter{
			&netlink.BpfFilter{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: 10, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 7, Protocol: unix.ETH_P_ALL,
				},
				DirectAction: true,
				Name:         "tc_eip.o:[classifier]",
			},
		},
	}
	netlinkOps = mock
	err = attachBPFFilter(tcFilterConfig{
		Link:        link,
		Program:     prog,
		AttachName:  "kube-ovn-eip",
		LegacyNames: []string{"tc_eip.o:[classifier]"},
		Priority:    5,
	})
	require.NoError(t, err)
	require.Len(t, mock.deletedFilters, 1)
	require.Equal(t, uint16(7), mock.deletedFilters[0].Attrs().Priority)
	require.Len(t, mock.replacedFilters, 1)
	require.Equal(t, uint16(5), mock.replacedFilters[0].Attrs().Priority)

	// 3. 同 priority 冲突分支：目标 priority 5 已被第三方规则占用（名字不匹配）
	mock = &mockTCNetlinkOps{
		filterListFilters: []netlink.Filter{
			&netlink.BpfFilter{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: 10, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 5, Protocol: unix.ETH_P_ALL,
				},
				DirectAction: true,
				Name:         "cilium-netdev",
			},
		},
	}
	netlinkOps = mock
	err = attachBPFFilter(tcFilterConfig{
		Link:        link,
		Program:     prog,
		AttachName:  "kube-ovn-eip",
		LegacyNames: []string{"tc_eip.o:[classifier]"},
		Priority:    5,
	})
	require.ErrorContains(t, err, "TC priority 5 on dummy0 is already owned by another filter")
	require.Empty(t, mock.replacedFilters)
	require.Empty(t, mock.deletedFilters)

	// 4. 消除 Ordering Hazard 分支：同 priority 5 已存在匹配本程序的规则，且排在后面的 priority 7 存在旧名称规则
	// 必须能够短路（不触发 FilterReplace），同时不能提前 return，必须成功剥离 priority 7 的旧规则
	mock = &mockTCNetlinkOps{
		filterListFilters: []netlink.Filter{
			&netlink.BpfFilter{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: 10, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 5, Protocol: unix.ETH_P_ALL,
				},
				DirectAction: true,
				Name:         "kube-ovn-eip",
			},
			&netlink.BpfFilter{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: 10, Parent: netlink.HANDLE_MIN_INGRESS, Handle: 1, Priority: 7, Protocol: unix.ETH_P_ALL,
				},
				DirectAction: true,
				Name:         "tc_eip.o:[classifier]",
			},
		},
	}
	netlinkOps = mock
	err = attachBPFFilter(tcFilterConfig{
		Link:        link,
		Program:     prog,
		AttachName:  "kube-ovn-eip",
		LegacyNames: []string{"tc_eip.o:[classifier]"},
		Priority:    5,
	})
	require.NoError(t, err)
	require.Empty(t, mock.replacedFilters)
	require.Len(t, mock.deletedFilters, 1)
	require.Equal(t, uint16(7), mock.deletedFilters[0].Attrs().Priority)
}

func TestIsMatchingTCFilterB1Regression(t *testing.T) {
	// 验证 B1 回归场景：
	// 旧版本可能保留了名字为 "tc_eip.o:[classifier]" 的 Filter 规则，
	// 且由于升级重编译，ProgTag 与内核 ProgID 均不相同。
	// 新版本必须仅通过 FilterNames 命中并识别该旧规则，以支持跨 Priority 安全剥离。
	legacyFilter := &netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			Handle:   1,
			Parent:   netlink.HANDLE_MIN_INGRESS,
			Protocol: unix.ETH_P_ALL,
			Priority: 5,
		},
		DirectAction: true,
		Id:           1001,                    // 旧程序在内核中的 ProgID
		Tag:          "aaaaaaaaaaaaaaaa",      // 旧程序字节码 Tag
		Name:         "tc_eip.o:[classifier]", // 历史版本规则名
	}

	newProgID := uint32(2002)
	newProgTag := "bbbbbbbbbbbbbbbb"
	filterNames := []string{tcEIPFilterName, "tc_eip.o:[classifier]"}

	// 1. 在 ProgID 与 ProgTag 完全不一致的情况下，必须通过历史 FilterName 成功识别旧规则（B1 核心诉求）
	require.True(t, isMatchingTCFilter(legacyFilter, newProgID, newProgTag, filterNames...))

	// 2. 若未配置历史 FilterName，则由于 ID/Tag 均不匹配而判定非本程序，无法识别
	require.False(t, isMatchingTCFilter(legacyFilter, newProgID, newProgTag, tcEIPFilterName))

	// 3. 第三方（如 Cilium/Calico）规则绝不误匹配
	alienFilter := &netlink.BpfFilter{
		FilterAttrs: netlink.FilterAttrs{
			Handle:   1,
			Parent:   netlink.HANDLE_MIN_INGRESS,
			Protocol: unix.ETH_P_ALL,
			Priority: 10,
		},
		DirectAction: true,
		Name:         "cilium-netdev",
	}
	require.False(t, isMatchingTCFilter(alienFilter, newProgID, newProgTag, filterNames...))
}
