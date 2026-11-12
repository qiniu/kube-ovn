package util

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	remotecommandconsts "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestExecuteWithOptionsContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("cancelled exec reached the API server")
	}))
	t.Cleanup(server.Close)
	cfg := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = ExecuteWithOptionsContext(ctx, client, cfg, ExecOptions{
		Namespace: "default", PodName: "gateway", ContainerName: "vpc-nat-gw", Command: []string{"nft", "-j", "list", "counters"},
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestExecuteWithOptionsContextCancelsActiveRequest(t *testing.T) {
	requested, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(requested)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	cfg := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := ExecuteWithOptionsContext(ctx, client, cfg, ExecOptions{
			Namespace: "default", PodName: "gateway", ContainerName: "vpc-nat-gw", Command: []string{"nft", "-j", "list", "counters"},
		})
		result <- err
	}()
	select {
	case <-requested:
	case <-time.After(5 * time.Second):
		t.Fatal("exec did not reach the API server")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("active exec did not stop after cancellation")
	}
}

func TestExecuteWithOptionsContextCancelsStreamingOutput(t *testing.T) {
	written, release := make(chan error, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := httpstream.Handshake(r, w, []string{remotecommandconsts.StreamProtocolV4Name}); err != nil {
			t.Error(err)
			return
		}
		conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(stream httpstream.Stream, replySent <-chan struct{}) error {
			if stream.Headers().Get(corev1.StreamType) == corev1.StreamTypeStdout {
				go func() {
					<-replySent
					payload := bytes.Repeat([]byte("x"), 64<<10)
					for range 256 {
						if _, err := stream.Write(payload); err != nil {
							written <- err
							return
						}
					}
					written <- nil
					for {
						if _, err := stream.Write(payload); err != nil {
							return
						}
					}
				}()
			}
			return nil
		})
		if conn != nil {
			defer conn.Close()
			<-release
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	cfg := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan struct {
		stdout string
		err    error
	}, 1)
	go func() {
		stdout, _, err := ExecuteWithOptionsContext(ctx, client, cfg, ExecOptions{
			Namespace: "default", PodName: "gateway", ContainerName: "vpc-nat-gw",
			Command: []string{"nft", "-j", "list", "counters"}, CaptureStdout: true,
		})
		result <- struct {
			stdout string
			err    error
		}{stdout, err}
	}()
	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("SPDY stdout did not start streaming")
	}
	cancel()
	select {
	case output := <-result:
		require.ErrorIs(t, output.err, context.Canceled)
		require.Zero(t, len(output.stdout), "cancelled exec must discard incomplete output")
	case <-time.After(5 * time.Second):
		t.Fatal("streaming exec did not stop after cancellation")
	}
}

func TestExecuteCommandInContainer(t *testing.T) {
	cfg := &rest.Config{}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	namespace := "default"
	podName := "pod"
	containerName := "container"
	cmd := "ls"
	stdout, stderr, err := ExecuteCommandInContainer(kubeClient, cfg, namespace, podName, containerName, cmd)
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
}

func TestExecuteWithOptions(t *testing.T) {
	cfg := &rest.Config{}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	namespace := "default"
	podName := "pod"
	containerName := "container"
	cmd := []string{"ls"}
	execOptions := ExecOptions{
		Command:            cmd,
		Namespace:          namespace,
		PodName:            podName,
		ContainerName:      containerName,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	}
	stdout, stderr, err := ExecuteWithOptions(kubeClient, cfg, execOptions)
	require.Error(t, err)
	require.Empty(t, stdout)
	require.Empty(t, stderr)
}

func TestExecute(t *testing.T) {
	cfg := &rest.Config{}
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	namespace := "default"
	podName := "pod"
	containerName := "container"
	options := ExecOptions{
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
		Stdin:         nil,
	}
	req := client.CoreV1().RESTClient().Post().
		Resource("xxxx").
		Name(options.PodName).
		Namespace(options.Namespace).
		SubResource("exec").
		Param("container", options.ContainerName)

	var stdout, stderr bytes.Buffer
	cfg.ExecProvider = &clientcmdapi.ExecConfig{APIVersion: "client.authentication.k8s.io/v1beta1"}
	cfg.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "exec"}
	err = execute("xxxx", req.URL(), cfg, options.Stdin, &stdout, &stderr, false)
	require.Error(t, err)
}
