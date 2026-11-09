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

func TestAttachBPFFilterEmptyFilterNames(t *testing.T) {
	err := attachBPFFilter(tcFilterConfig{
		Link:    &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "dummy0"}},
		Program: &ebpf.Program{},
	})
	require.ErrorContains(t, err, "FilterNames must have at least one non-empty name as attach name")
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
