package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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

		env kvList

		timeout = flag.Duration("timeout", 30*time.Minute, "overall timeout (job register -> alloc complete)")
		keepJob = flag.Bool("keep-job", false, "do not deregister job after completion")
		dryRun  = flag.Bool("dry-run", false, "print computed job and exit without contacting Nomad")
	)

	flag.Var(&datacenters, "datacenter", "datacenter (repeatable or comma-separated)")
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

	jobID, err := makeJobID(*jobPrefix)
	if err != nil {
		exitErr(err)
	}

	job, err := buildJob(jobID, *namespace, *region, datacenters, *taskName, *driver, *dockerImage, cmdArgs, envMap, *cpu, *memory)
	if err != nil {
		exitErr(err)
	}

	if strings.TrimSpace(*nodeIP) != "" {
		ip := strings.TrimSpace(*nodeIP)
		if len(job.TaskGroups) == 0 {
			exitErr(errors.New("unexpected job shape (no task groups)"))
		}
		job.TaskGroups[0].Constraints = append(job.TaskGroups[0].Constraints, nomadapi.NewConstraint("${attr.unique.network.ip-address}", "=", ip))
	}

	if *dryRun {
		fmt.Printf("job: %s\n", jobID)
		fmt.Printf("address: %s\n", *addr)
		if strings.TrimSpace(*nodeIP) != "" {
			fmt.Printf("node-ip: %s\n", strings.TrimSpace(*nodeIP))
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

func buildJob(jobID, namespace, region string, datacenters []string, taskName, driver, dockerImage string, cmdArgs []string, env map[string]string, cpu, memory int) (*nomadapi.Job, error) {
	if len(cmdArgs) == 0 {
		return nil, errors.New("empty command")
	}
	if driver != "raw_exec" && driver != "docker" {
		return nil, fmt.Errorf("unsupported -driver %q (expected raw_exec or docker)", driver)
	}
	if driver == "docker" && strings.TrimSpace(dockerImage) == "" {
		return nil, errors.New("-docker-image is required when -driver=docker")
	}
	if len(datacenters) == 0 {
		datacenters = []string{"dc1"}
	}

	config := map[string]any{}
	switch driver {
	case "raw_exec":
		config["command"] = cmdArgs[0]
		if len(cmdArgs) > 1 {
			config["args"] = cmdArgs[1:]
		}
	case "docker":
		config["image"] = dockerImage
		config["command"] = cmdArgs[0]
		if len(cmdArgs) > 1 {
			config["args"] = cmdArgs[1:]
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
						Name:   taskName,
						Driver: driver,
						Config: config,
						Env:    env,
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
