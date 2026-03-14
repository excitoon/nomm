package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	nomadapi "github.com/hashicorp/nomad/api"
)

// `nomm` is a tiny single-file Nomad “remote runner”.
//
// Contract (high level):
// - Registers a short-lived batch job that runs a single command.
// - Waits for the allocation to complete.
// - Best-effort streams stdout/stderr when available via AllocFS.
// - Exits with the task's exit code (or non-zero on errors).

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	if v == "" {
		return nil
	}
	for _, part := range strings.Split(v, ",") {
		p := strings.TrimSpace(part)
		if p != "" {
			*s = append(*s, p)
		}
	}
	return nil
}

type kvList []string

func (k *kvList) String() string { return strings.Join(*k, ",") }
func (k *kvList) Set(v string) error {
	if v == "" {
		return nil
	}
	*k = append(*k, v)
	return nil
}

const remoteWorkRoot = "task/local/nomm/work"

type uploadedInput struct {
	rel        string
	data       []byte
	executable bool
}

type execLayout struct {
	inputPrefix      string
	outputPrefix     string
	workingDirectory string
	inputs           []uploadedInput
	outputs          []string
}

type execTarget struct {
	nodeID  string
	kernel  string
	address string
}

func (l execLayout) needsWrapper() bool {
	return len(l.inputs) > 0 || len(l.outputs) > 0 || l.workingDirectory != ""
}

func main() {
	defaultAddr := "http://127.0.0.1:4646"
	if v := strings.TrimSpace(os.Getenv("NOMAD_ADDR")); v != "" {
		defaultAddr = v
	}
	defaultToken := strings.TrimSpace(os.Getenv("NOMAD_TOKEN"))

	var (
		addr      = flag.String("address", defaultAddr, "Nomad address (e.g. http(s)://host:4646); defaults to $NOMAD_ADDR if set")
		token     = flag.String("token", defaultToken, "Nomad ACL token (optional); defaults to $NOMAD_TOKEN if set")
		namespace = flag.String("namespace", "", "Nomad namespace (optional)")
		region    = flag.String("region", "", "Nomad region (optional)")

		datacenters = stringList{}
		nodeIP      = flag.String("node-ip", "", "pin placement to Nomad client IP (matches ${attr.unique.network.ip-address})")

		tlsCAPath     = flag.String("tls-ca", "", "path to PEM CA certificate to trust for TLS (optional)")
		tlsCertPath   = flag.String("tls-cert", "", "path to PEM client certificate (optional)")
		tlsKeyPath    = flag.String("tls-key", "", "path to PEM client key (optional)")
		tlsServer     = flag.String("tls-server-name", "", "TLS server name override (optional)")
		tlsSkipVerify = flag.Bool("tls-insecure-skip-verify", false, "skip TLS hostname/SAN verification (insecure)")

		driver      = flag.String("driver", "raw_exec", "Nomad task driver: raw_exec or docker")
		dockerImage = flag.String("docker-image", "", "Docker image (required when -driver=docker)")

		jobPrefix = flag.String("job-prefix", "nomm", "job name prefix")
		taskName  = flag.String("task", "task", "task name")
		cpu       = flag.Int("cpu", 100, "CPU MHz")
		memory    = flag.Int("memory", 128, "memory MB")

		inputs  = stringList{}
		outputs = stringList{}
		workDir = flag.String("working-directory", "", "remote working directory relative to uploaded input root (optional)")

		env kvList

		timeout = flag.Duration("timeout", 30*time.Minute, "overall timeout (job register -> alloc complete)")
		keepJob = flag.Bool("keep-job", false, "do not deregister job after completion")
		dryRun  = flag.Bool("dry-run", false, "print computed job and exit without contacting Nomad")
	)

	flag.Var(&datacenters, "datacenter", "datacenter (repeatable or comma-separated)")
	flag.Var(&inputs, "input", "input file (repeatable or comma-separated)")
	flag.Var(&outputs, "output", "expected output file (repeatable or comma-separated)")
	flag.Var(&env, "environment", "environment var KEY=VALUE (repeatable)")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [nomad flags] -- command [args...]\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	cmdArgs := flag.Args()
	if len(cmdArgs) == 0 {
		exitf("command is required after --")
	}

	envMap, err := parseEnv(env)
	if err != nil {
		exitErr(err)
	}

	layout, err := buildExecLayout(inputs, outputs, *workDir)
	if err != nil {
		exitErr(err)
	}

	jobID, err := makeJobID(*jobPrefix)
	if err != nil {
		exitErr(err)
	}

	if *dryRun {
		fmt.Printf("job: %s\n", jobID)
		fmt.Printf("address: %s\n", *addr)
		if strings.TrimSpace(*nodeIP) != "" {
			fmt.Printf("node-ip: %s\n", strings.TrimSpace(*nodeIP))
		}
		fmt.Printf("input prefix: %s\n", layout.inputPrefix)
		fmt.Printf("output prefix: %s\n", layout.outputPrefix)
		if layout.workingDirectory != "" {
			fmt.Printf("working-directory: %s\n", layout.workingDirectory)
		}
		fmt.Printf("driver: %s\n", *driver)
		fmt.Printf("command: %q\n", cmdArgs)
		keys := make([]string, 0, len(envMap))
		for k := range envMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("env: %s=%s\n", k, envMap[k])
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := newNomadClient(*addr, *token, *namespace, *region, *tlsCAPath, *tlsCertPath, *tlsKeyPath, *tlsServer, *tlsSkipVerify)
	if err != nil {
		exitErr(err)
	}

	target, err := resolveExecTarget(ctx, client, datacenters, strings.TrimSpace(*nodeIP), *driver, layout.needsWrapper())
	if err != nil {
		exitErr(err)
	}

	job, err := buildJob(jobID, *namespace, *region, datacenters, *taskName, *driver, *dockerImage, cmdArgs, envMap, *cpu, *memory, layout, target)
	if err != nil {
		exitErr(err)
	}

	if target.nodeID != "" {
		if len(job.TaskGroups) == 0 {
			exitErr(errors.New("unexpected job shape (no task groups)"))
		}
		job.TaskGroups[0].Constraints = append(job.TaskGroups[0].Constraints, nomadapi.NewConstraint("${node.unique.id}", "=", target.nodeID))
	}

	jobs := client.Jobs()
	registerResp, _, err := jobs.Register(job, nil)
	if err != nil {
		exitErr(err)
	}
	if registerResp == nil || registerResp.EvalID == "" {
		exitErr(errors.New("unexpected register response (missing eval ID)"))
	}

	evalID := registerResp.EvalID
	alloc, err := waitForAllocation(ctx, client, evalID)
	if err != nil {
		if !*keepJob {
			_ = deregisterJobBestEffort(client, jobID)
		}
		exitErr(err)
	}

	_ = printLogsBestEffort(ctx, client, alloc, *taskName)

	exitCode, err := allocationExitCode(alloc, *taskName)
	if err != nil {
		if !*keepJob {
			_ = deregisterJobBestEffort(client, jobID)
		}
		exitErr(err)
	}

	if exitCode == 0 && len(layout.outputs) > 0 {
		if err := downloadOutputs(client, alloc, layout.outputs, layout.outputPrefix, layout.workingDirectory); err != nil {
			if !*keepJob {
				_ = deregisterJobBestEffort(client, jobID)
			}
			exitErr(err)
		}
	}

	if !*keepJob {
		_ = deregisterJobBestEffort(client, jobID)
	}

	if exitCode != 0 {
		fmt.Fprintf(os.Stderr, "remote task failed with exit code %d\n", exitCode)
		os.Exit(exitCode)
	}
}

func exitf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

func exitErr(err error) {
	exitf("error: %v", err)
}

func makeJobID(prefix string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	suffix := hex.EncodeToString(b)
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "nomm"
	}
	return fmt.Sprintf("%s-%s", p, suffix), nil
}

func parseEnv(env kvList) (map[string]string, error) {
	envMap := map[string]string{}
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid -environment %q, expected KEY=VALUE", e)
		}
		k := parts[0]
		if k == "" {
			return nil, fmt.Errorf("invalid -environment %q, empty key", e)
		}
		envMap[k] = parts[1]
	}
	return envMap, nil
}

func buildJob(jobID, namespace, region string, datacenters []string, taskName, driver, dockerImage string, cmdArgs []string, env map[string]string, cpu, memory int, layout execLayout, target execTarget) (*nomadapi.Job, error) {
	if len(cmdArgs) == 0 {
		return nil, errors.New("empty command")
	}
	if driver != "raw_exec" && driver != "docker" {
		return nil, fmt.Errorf("unsupported -driver %q (expected raw_exec or docker)", driver)
	}
	if layout.needsWrapper() && driver != "raw_exec" {
		return nil, errors.New("-input, -output, and -working-directory currently require -driver=raw_exec")
	}
	if driver == "docker" && strings.TrimSpace(dockerImage) == "" {
		return nil, errors.New("-docker-image is required when -driver=docker")
	}
	if len(datacenters) == 0 {
		datacenters = []string{"dc1"}
	}

	config := map[string]any{}
	var templates []*nomadapi.Template
	switch driver {
	case "raw_exec":
		if layout.needsWrapper() {
			switch target.kernel {
			case "windows":
				wrapper := buildWindowsWrapper(layout)
				templates = append(templates, &nomadapi.Template{
					DestPath:     strPtr("local/nomm/run.cmd"),
					EmbeddedTmpl: strPtr(wrapper),
					Perms:        strPtr("0755"),
					Once:         boolPtr(true),
				})
				config["command"] = `C:\Windows\System32\cmd.exe`
				config["args"] = buildWindowsWrapperArgs(cmdArgs)
			case "linux", "darwin":
				wrapper, err := buildPOSIXWrapper(layout)
				if err != nil {
					return nil, err
				}
				templates = append(templates, &nomadapi.Template{
					DestPath:     strPtr("local/nomm/run.sh"),
					EmbeddedTmpl: strPtr(wrapper),
					Perms:        strPtr("0755"),
					Once:         boolPtr(true),
				})
				config["command"] = "/bin/sh"
				config["args"] = buildPOSIXWrapperArgs(cmdArgs)
			default:
				return nil, fmt.Errorf("unsupported raw_exec target OS %q for uploaded inputs/outputs", target.kernel)
			}
		} else {
			config["command"] = cmdArgs[0]
			if len(cmdArgs) > 1 {
				config["args"] = cmdArgs[1:]
			}
		}
	case "docker":
		config["image"] = dockerImage
		if layout.needsWrapper() {
			wrapper, err := buildPOSIXWrapper(layout)
			if err != nil {
				return nil, err
			}
			templates = append(templates, &nomadapi.Template{
				DestPath:     strPtr("local/nomm/run.sh"),
				EmbeddedTmpl: strPtr(wrapper),
				Perms:        strPtr("0755"),
				Once:         boolPtr(true),
			})
			config["command"] = "/bin/sh"
			config["args"] = buildPOSIXWrapperArgs(cmdArgs)
		} else {
			config["command"] = cmdArgs[0]
			if len(cmdArgs) > 1 {
				config["args"] = cmdArgs[1:]
			}
		}
	}

	jobType := "batch"
	prio := 50
	allAtOnce := false
	attempts := 0
	delay := 0 * time.Second
	mode := "fail"

	j := &nomadapi.Job{
		ID:          strPtr(jobID),
		Name:        strPtr(jobID),
		Type:        strPtr(jobType),
		Priority:    intPtr(prio),
		AllAtOnce:   boolPtr(allAtOnce),
		Namespace:   strPtrOrNil(namespace),
		Region:      strPtrOrNil(region),
		Datacenters: datacenters,
		TaskGroups: []*nomadapi.TaskGroup{
			{
				Name:  strPtr("group"),
				Count: intPtr(1),
				RestartPolicy: &nomadapi.RestartPolicy{
					Attempts: &attempts,
					Delay:    &delay,
					Mode:     &mode,
				},
				Tasks: []*nomadapi.Task{
					{
						Name:      taskName,
						Driver:    driver,
						Config:    config,
						Env:       env,
						Templates: templates,
						Resources: &nomadapi.Resources{
							CPU:      intPtr(cpu),
							MemoryMB: intPtr(memory),
						},
					},
				},
			},
		},
	}
	return j, nil
}

func newNomadClient(address, token, namespace, region, tlsCAPath, tlsCertPath, tlsKeyPath, serverName string, insecureSkipVerify bool) (*nomadapi.Client, error) {
	cfg := nomadapi.DefaultConfig()
	cfg.Address = address
	if token != "" {
		cfg.SecretID = token
	}
	if namespace != "" {
		cfg.Namespace = namespace
	}
	if region != "" {
		cfg.Region = region
	}

	if tlsCAPath != "" || tlsCertPath != "" || tlsKeyPath != "" || serverName != "" || insecureSkipVerify {
		if (tlsCertPath == "") != (tlsKeyPath == "") {
			return nil, errors.New("-tls-cert and -tls-key must be provided together")
		}
		cfg.TLSConfig = &nomadapi.TLSConfig{
			CACert:        tlsCAPath,
			ClientCert:    tlsCertPath,
			ClientKey:     tlsKeyPath,
			TLSServerName: serverName,
			Insecure:      insecureSkipVerify,
		}
	}

	return nomadapi.NewClient(cfg)
}

func waitForAllocation(ctx context.Context, client *nomadapi.Client, evalID string) (*nomadapi.Allocation, error) {
	evals := client.Evaluations()
	allocs := client.Allocations()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
			eval, _, err := evals.Info(evalID, nil)
			if err != nil {
				return nil, err
			}
			if eval == nil {
				continue
			}
			if eval.Status != "complete" {
				continue
			}

			evalAllocs, _, err := evals.Allocations(evalID, nil)
			if err != nil {
				return nil, err
			}
			if len(evalAllocs) == 0 {
				continue
			}

			allocID := evalAllocs[0].ID
			for {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tick.C:
					al, _, err := allocs.Info(allocID, nil)
					if err != nil {
						return nil, err
					}
					if al == nil {
						continue
					}
					if al.ClientStatus == "complete" || al.ClientStatus == "failed" {
						return al, nil
					}
				}
			}
		}
	}
}

func allocationExitCode(alloc *nomadapi.Allocation, taskName string) (int, error) {
	if alloc == nil {
		return 0, errors.New("nil allocation")
	}
	ts, ok := alloc.TaskStates[taskName]
	if !ok || ts == nil {
		return 0, fmt.Errorf("task %q not found in allocation", taskName)
	}
	if len(ts.Events) == 0 {
		if ts.Failed {
			return 1, errors.New("task failed (no events available for exit code)")
		}
		return 0, nil
	}

	for i := len(ts.Events) - 1; i >= 0; i-- {
		ev := ts.Events[i]
		if ev == nil {
			continue
		}
		if ev.Type == nomadapi.TaskTerminated || ev.Type == nomadapi.TaskKilled {
			return ev.ExitCode, nil
		}
	}

	if ts.Failed {
		return 1, errors.New("task failed (exit code not reported)")
	}
	return 0, nil
}

func printLogsBestEffort(ctx context.Context, client *nomadapi.Client, alloc *nomadapi.Allocation, taskName string) error {
	if alloc == nil {
		return nil
	}
	fs := client.AllocFS()

	stream := func(logType string, w io.Writer) {
		frames, errs := fs.Logs(alloc, false, taskName, logType, "start", 0, ctx.Done(), nil)
		if frames == nil {
			// Drain the error if present and ignore.
			if errs != nil {
				select {
				case <-errs:
				default:
				}
			}
			return
		}
		for {
			select {
			case f, ok := <-frames:
				if !ok {
					return
				}
				if f != nil && len(f.Data) > 0 {
					_, _ = w.Write(f.Data)
				}
			case <-errs:
				// Best-effort; ignore errors.
			case <-ctx.Done():
				return
			}
		}
	}

	stream("stdout", os.Stdout)
	stream("stderr", os.Stderr)
	return nil
}

func deregisterJobBestEffort(client *nomadapi.Client, jobID string) error {
	_, _, err := client.Jobs().Deregister(jobID, false, nil)
	return err
}

func strPtr(v string) *string { return &v }

func strPtrOrNil(v string) *string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return &v
}

func intPtr(v int) *int { return &v }

func boolPtr(v bool) *bool { return &v }

func buildExecLayout(inputs, outputs []string, workingDirectory string) (execLayout, error) {
	inPrefix, err := mostCommonDir(inputs)
	if err != nil {
		return execLayout{}, err
	}
	outPrefix, err := mostCommonDir(outputs)
	if err != nil {
		return execLayout{}, err
	}
	cleanWorkDir, err := cleanRelativePath(workingDirectory)
	if err != nil {
		return execLayout{}, fmt.Errorf("invalid -working-directory: %w", err)
	}
	ins, err := collectInputs(inPrefix, inputs)
	if err != nil {
		return execLayout{}, err
	}
	return execLayout{
		inputPrefix:      inPrefix,
		outputPrefix:     outPrefix,
		workingDirectory: cleanWorkDir,
		inputs:           ins,
		outputs:          append([]string(nil), outputs...),
	}, nil
}

func mostCommonDir(paths []string) (string, error) {
	counts := map[string]int{}
	for _, p := range paths {
		c := filepath.Clean(p)
		if c == "." || c == string(filepath.Separator) {
			continue
		}
		d := filepath.Dir(c)
		if d == "." {
			d = ""
		}
		counts[filepath.Clean(d)]++
	}
	if len(counts) == 0 {
		return "", nil
	}
	best := ""
	bestN := -1
	for d, n := range counts {
		if n > bestN || (n == bestN && len(d) > len(best)) {
			best = d
			bestN = n
		}
	}
	if best == "." {
		best = ""
	}
	return best, nil
}

func collectInputs(prefix string, inputs []string) ([]uploadedInput, error) {
	files := make([]uploadedInput, 0, len(inputs))
	for _, in := range inputs {
		p := filepath.Clean(in)
		st, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if st.IsDir() {
			return nil, fmt.Errorf("input %q is a directory; only files are supported", in)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var rel string
		if prefix == "" {
			rel = p
			if filepath.IsAbs(rel) {
				return nil, fmt.Errorf("input %q is absolute; provide relative -input paths or a shared parent directory", p)
			}
		} else {
			r, err := filepath.Rel(prefix, p)
			if err != nil {
				return nil, err
			}
			rel = r
		}
		rel, err = cleanRelativePath(rel)
		if err != nil {
			return nil, fmt.Errorf("input %q is not under prefix %q", p, prefix)
		}
		files = append(files, uploadedInput{
			rel:        filepath.ToSlash(rel),
			data:       data,
			executable: st.Mode()&0o111 != 0,
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, nil
}

func cleanRelativePath(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", nil
	}
	if filepath.IsAbs(p) {
		return "", errors.New("must be relative")
	}
	c := filepath.ToSlash(filepath.Clean(p))
	if c == "." {
		return "", nil
	}
	if c == ".." || strings.HasPrefix(c, "../") {
		return "", errors.New("must stay within the uploaded input root")
	}
	return c, nil
}

func buildPOSIXWrapperArgs(cmdArgs []string) []string {
	args := []string{"-lc", `exec "$NOMAD_TASK_DIR/nomm/run.sh" "$@"`, "nomm-run"}
	args = append(args, cmdArgs...)
	return args
}

func buildWindowsWrapperArgs(cmdArgs []string) []string {
	args := []string{"/C", `local\nomm\run.cmd`}
	args = append(args, cmdArgs...)
	return args
}

func buildPOSIXWrapper(layout execLayout) (string, error) {
	var b strings.Builder
	b.WriteString("#!/bin/sh\n")
	b.WriteString("set -eu\n\n")
	b.WriteString("task_dir=\"${NOMAD_TASK_DIR:-$PWD}\"\n")
	b.WriteString("work_root=\"$task_dir/nomm/work\"\n")
	b.WriteString("rm -rf \"$work_root\"\n")
	b.WriteString("mkdir -p \"$work_root\"\n")

	dirs := map[string]struct{}{}
	for _, in := range layout.inputs {
		dir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(in.rel)))
		if dir == "." {
			continue
		}
		dirs[dir] = struct{}{}
	}
	dirList := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirList = append(dirList, dir)
	}
	sort.Strings(dirList)
	for _, dir := range dirList {
		fmt.Fprintf(&b, "mkdir -p \"$work_root/%s\"\n", shellDoubleQuoteEscape(dir))
	}

	for _, in := range layout.inputs {
		remotePath := shellDoubleQuoteEscape(in.rel)
		fmt.Fprintf(&b, ": > \"$work_root/%s\"\n", remotePath)
		for _, chunk := range octalChunks(in.data, 768) {
			fmt.Fprintf(&b, "printf '%%b' '%s' >> \"$work_root/%s\"\n", chunk, remotePath)
		}
		if in.executable {
			fmt.Fprintf(&b, "chmod 755 \"$work_root/%s\"\n", remotePath)
		} else {
			fmt.Fprintf(&b, "chmod 644 \"$work_root/%s\"\n", remotePath)
		}
	}

	if layout.workingDirectory != "" {
		escapedWorkDir := shellDoubleQuoteEscape(layout.workingDirectory)
		fmt.Fprintf(&b, "mkdir -p \"$work_root/%s\"\n", escapedWorkDir)
		fmt.Fprintf(&b, "cd \"$work_root/%s\"\n", escapedWorkDir)
	} else {
		b.WriteString("cd \"$work_root\"\n")
	}
	b.WriteString("exec \"$@\"\n")
	return b.String(), nil
}

func buildWindowsWrapper(layout execLayout) string {
	var b strings.Builder
	b.WriteString("@echo off\r\n")
	b.WriteString("setlocal EnableExtensions DisableDelayedExpansion\r\n")
	b.WriteString("set \"TASK_DIR=%NOMAD_TASK_DIR%\"\r\n")
	b.WriteString("if not defined TASK_DIR set \"TASK_DIR=%CD%\"\r\n")
	b.WriteString("set \"WORK_ROOT=%TASK_DIR%\\nomm\\work\"\r\n")
	b.WriteString("if exist \"%WORK_ROOT%\" rmdir /s /q \"%WORK_ROOT%\"\r\n")
	b.WriteString("mkdir \"%WORK_ROOT%\" || exit /b 1\r\n")

	dirs := map[string]struct{}{}
	for _, in := range layout.inputs {
		dir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(in.rel)))
		if dir == "." {
			continue
		}
		dirs[dir] = struct{}{}
	}
	dirList := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirList = append(dirList, dir)
	}
	sort.Strings(dirList)
	for _, dir := range dirList {
		fmt.Fprintf(&b, "mkdir \"%%WORK_ROOT%%\\%s\" 2>nul\r\n", windowsBatchEscapePath(dir))
	}

	for _, in := range layout.inputs {
		relPath := windowsBatchEscapePath(in.rel)
		base64Path := relPath + ".b64"
		fmt.Fprintf(&b, "break > \"%%WORK_ROOT%%\\%s\"\r\n", base64Path)
		for _, line := range base64Lines(in.data, 76) {
			fmt.Fprintf(&b, "echo %s>> \"%%WORK_ROOT%%\\%s\"\r\n", line, base64Path)
		}
		fmt.Fprintf(&b, "certutil -f -decode \"%%WORK_ROOT%%\\%s\" \"%%WORK_ROOT%%\\%s\" >nul || exit /b 1\r\n", base64Path, relPath)
		fmt.Fprintf(&b, "del /q \"%%WORK_ROOT%%\\%s\"\r\n", base64Path)
	}

	if layout.workingDirectory != "" {
		fmt.Fprintf(&b, "mkdir \"%%WORK_ROOT%%\\%s\" 2>nul\r\n", windowsBatchEscapePath(layout.workingDirectory))
		fmt.Fprintf(&b, "cd /d \"%%WORK_ROOT%%\\%s\" || exit /b 1\r\n", windowsBatchEscapePath(layout.workingDirectory))
	} else {
		b.WriteString("cd /d \"%WORK_ROOT%\" || exit /b 1\r\n")
	}
	b.WriteString("%*\r\n")
	b.WriteString("exit /b %ERRORLEVEL%\r\n")
	return b.String()
}

func octalChunks(data []byte, chunkSize int) []string {
	if len(data) == 0 {
		return nil
	}
	chunks := make([]string, 0, (len(data)+chunkSize-1)/chunkSize)
	for start := 0; start < len(data); start += chunkSize {
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		var b strings.Builder
		b.Grow((end - start) * 4)
		for _, ch := range data[start:end] {
			fmt.Fprintf(&b, "\\%03o", ch)
		}
		chunks = append(chunks, b.String())
	}
	return chunks
}

func base64Lines(data []byte, lineLen int) []string {
	enc := base64.StdEncoding.EncodeToString(data)
	if enc == "" {
		return []string{""}
	}
	lines := make([]string, 0, (len(enc)+lineLen-1)/lineLen)
	for start := 0; start < len(enc); start += lineLen {
		end := start + lineLen
		if end > len(enc) {
			end = len(enc)
		}
		lines = append(lines, enc[start:end])
	}
	return lines
}

func shellDoubleQuoteEscape(s string) string {
	replacer := strings.NewReplacer(
		`\\`, `\\\\`,
		`"`, `\\"`,
		`$`, `\\$`,
		"`", "\\`",
	)
	return replacer.Replace(s)
}

func windowsBatchEscapePath(s string) string {
	s = strings.ReplaceAll(s, "/", `\`)
	s = strings.ReplaceAll(s, `%`, `%%`)
	return s
}

func resolveExecTarget(ctx context.Context, client *nomadapi.Client, datacenters []string, nodeIP, driver string, needsWrapper bool) (execTarget, error) {
	if strings.TrimSpace(nodeIP) == "" && !needsWrapper {
		return execTarget{}, nil
	}

	stubs, _, err := client.Nodes().List(nil)
	if err != nil {
		return execTarget{}, err
	}
	allowedDC := map[string]bool{}
	for _, dc := range datacenters {
		if dc != "" {
			allowedDC[dc] = true
		}
	}

	candidates := make([]*nomadapi.NodeListStub, 0, len(stubs))
	for _, stub := range stubs {
		if stub == nil {
			continue
		}
		if len(allowedDC) > 0 && !allowedDC[stub.Datacenter] {
			continue
		}
		if stub.Status != nomadapi.NodeStatusReady || stub.SchedulingEligibility != nomadapi.NodeSchedulingEligible {
			continue
		}
		if driver != "" {
			di := stub.Drivers[driver]
			if di == nil || !di.Detected || !di.Healthy {
				continue
			}
		}
		candidates = append(candidates, stub)
	}
	if len(candidates) == 0 {
		return execTarget{}, fmt.Errorf("no eligible Nomad nodes found for driver %q", driver)
	}

	if nodeIP != "" {
		for _, stub := range candidates {
			if stub.Address == nodeIP {
				return hydrateExecTarget(ctx, client, stub.ID, true)
			}
		}
		for _, stub := range candidates {
			info, err := hydrateExecTarget(ctx, client, stub.ID, true)
			if err != nil {
				return execTarget{}, err
			}
			if info.address == nodeIP {
				return info, nil
			}
		}
		return execTarget{}, fmt.Errorf("no eligible node matched -node-ip %q", nodeIP)
	}

	if !needsWrapper {
		return execTarget{}, nil
	}

	var chosen execTarget
	for i, stub := range candidates {
		info, err := hydrateExecTarget(ctx, client, stub.ID, false)
		if err != nil {
			return execTarget{}, err
		}
		if i == 0 {
			chosen = info
			continue
		}
		if info.kernel != chosen.kernel {
			return execTarget{}, errors.New("eligible raw_exec nodes have mixed operating systems; use -node-ip to select a specific node")
		}
	}
	return chosen, nil
}

func hydrateExecTarget(_ context.Context, client *nomadapi.Client, nodeID string, pinNode bool) (execTarget, error) {
	node, _, err := client.Nodes().Info(nodeID, nil)
	if err != nil {
		return execTarget{}, err
	}
	if node == nil {
		return execTarget{}, fmt.Errorf("node %q not found", nodeID)
	}
	kernel := strings.ToLower(strings.TrimSpace(node.Attributes["kernel.name"]))
	address := strings.TrimSpace(node.Attributes["unique.network.ip-address"])
	if address == "" {
		address = strings.TrimSpace(node.HTTPAddr)
	}
	target := execTarget{kernel: kernel, address: address}
	if pinNode {
		target.nodeID = node.ID
	}
	return target, nil
}

func downloadOutputs(client *nomadapi.Client, alloc *nomadapi.Allocation, outputs []string, outputPrefix, workingDirectory string) error {
	fs := client.AllocFS()
	for _, out := range outputs {
		want := filepath.ToSlash(strings.TrimPrefix(filepath.Clean(out), string(filepath.Separator)))
		candidates := buildOutputLookupCandidates(want, outputPrefix, workingDirectory)

		var remoteRel string
		found := false
		for _, candidate := range candidates {
			remotePath := filepath.ToSlash(filepath.Join(remoteWorkRoot, filepath.FromSlash(candidate)))
			info, _, err := fs.Stat(alloc, remotePath, nil)
			if err == nil && info != nil && !info.IsDir {
				remoteRel = candidate
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("output %q not found in allocation filesystem", out)
		}

		rel := localOutputPath(want, outputPrefix)
		dest := filepath.FromSlash(rel)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}

		remotePath := filepath.ToSlash(filepath.Join(remoteWorkRoot, filepath.FromSlash(remoteRel)))
		r, err := fs.Cat(alloc, remotePath, nil)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if err := os.WriteFile(dest, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func buildOutputLookupCandidates(want, outputPrefix, workingDirectory string) []string {
	seen := map[string]struct{}{}
	var candidates []string
	add := func(p string) {
		p = filepath.ToSlash(filepath.Clean(p))
		if p == "." || p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		candidates = append(candidates, p)
	}

	add(want)
	if outputPrefix != "" {
		prefix := filepath.ToSlash(filepath.Clean(outputPrefix)) + "/"
		if trimmed := strings.TrimPrefix(want, prefix); trimmed != want && trimmed != "" {
			add(trimmed)
		}
	}

	base := append([]string(nil), candidates...)
	if workingDirectory != "" {
		for _, candidate := range base {
			add(filepath.ToSlash(filepath.Join(workingDirectory, filepath.FromSlash(candidate))))
		}
	}

	if workingDirectory != "" {
		prefixed := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			wdPrefix := filepath.ToSlash(filepath.Clean(workingDirectory)) + "/"
			if strings.HasPrefix(candidate, wdPrefix) {
				prefixed = append(prefixed, candidate)
			}
		}
		others := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			wdPrefix := filepath.ToSlash(filepath.Clean(workingDirectory)) + "/"
			if !strings.HasPrefix(candidate, wdPrefix) {
				others = append(others, candidate)
			}
		}
		return append(prefixed, others...)
	}

	return candidates
}

func localOutputPath(want, outputPrefix string) string {
	rel := want
	if outputPrefix != "" {
		prefix := filepath.ToSlash(filepath.Clean(outputPrefix)) + "/"
		rel = strings.TrimPrefix(rel, prefix)
	}
	if rel == "" {
		rel = filepath.Base(filepath.FromSlash(want))
	}
	return rel
}
