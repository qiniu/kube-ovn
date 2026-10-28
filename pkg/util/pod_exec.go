package util

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	httpstreamspdy "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog/v2"
)

type ExecOptions struct {
	Command            []string
	Namespace          string
	PodName            string
	ContainerName      string
	Stdin              io.Reader
	CaptureStdout      bool
	CaptureStderr      bool
	PreserveWhitespace bool
}

func ExecuteCommandInContainer(client kubernetes.Interface, cfg *rest.Config, namespace, podName, containerName string, cmd ...string) (
	string, string, error,
) {
	return ExecuteWithOptions(client, cfg, ExecOptions{
		Command:            cmd,
		Namespace:          namespace,
		PodName:            podName,
		ContainerName:      containerName,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: false,
	})
}

func ExecuteWithOptions(client kubernetes.Interface, cfg *rest.Config, options ExecOptions) (string, string, error) {
	return ExecuteWithOptionsContext(context.Background(), client, cfg, options)
}

func ExecuteWithOptionsContext(ctx context.Context, client kubernetes.Interface, cfg *rest.Config, options ExecOptions) (string, string, error) {
	req := client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(options.PodName).
		Namespace(options.Namespace).
		SubResource("exec").
		Param("container", options.ContainerName)

	req.VersionedParams(&corev1.PodExecOptions{
		Stdin:     options.Stdin != nil,
		Stdout:    options.CaptureStdout,
		Stderr:    options.CaptureStderr,
		TTY:       false,
		Container: options.ContainerName,
		Command:   options.Command,
	}, scheme.ParameterCodec)

	var stdout, stderr bytes.Buffer
	err := executeContext(ctx, "POST", req.URL(), cfg, options.Stdin, &stdout, &stderr, false)
	// On cancellation, client-go's output copiers may still be finishing.
	if ctx.Err() != nil {
		return "", "", ctx.Err()
	}
	if options.PreserveWhitespace {
		return stdout.String(), stderr.String(), err
	}
	return strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()), err
}

func execute(method string, url *url.URL, cfg *rest.Config, stdin io.Reader, stdout, stderr io.Writer,
	tty bool,
) error {
	return executeContext(context.Background(), method, url, cfg, stdin, stdout, stderr, tty)
}

func executeContext(ctx context.Context, method string, url *url.URL, cfg *rest.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool) error {
	var exec remotecommand.Executor
	var err error
	if ctx.Done() == nil {
		exec, err = remotecommand.NewSPDYExecutor(cfg, method, url)
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		wrapper, upgrader, transportErr := spdy.RoundTripperFor(cfg)
		if transportErr != nil {
			return transportErr
		}
		// SPDY's proxy/upgrade reads ignore cancellation after dialing. Shut down
		// the owned socket to unblock them; RawConn keeps a reused fd untouched.
		upgrader.(*httpstreamspdy.SpdyRoundTripper).Dialer = &net.Dialer{
			ControlContext: func(_ context.Context, _, _ string, raw syscall.RawConn) error {
				context.AfterFunc(ctx, func() {
					_ = raw.Control(func(fd uintptr) {
						_ = syscall.Shutdown(int(fd), syscall.SHUT_RDWR) // #nosec G115 -- OS file descriptors fit int.
					})
				})
				return nil
			},
		}
		exec, err = remotecommand.NewSPDYExecutorForTransports(wrapper, upgrader, method, url)
	}
	if err != nil {
		klog.Errorf("remotecommand.NewSPDYExecutor error: %v", err)
		return err
	}
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    tty,
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
