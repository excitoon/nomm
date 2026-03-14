A tiny single-file Go client for **HashiCorp Nomad**: run one command as a short-lived batch job and get its exit code back locally.

# nomm

It:
- Registers a short-lived **batch** job in Nomad to run one command
- Streams task output best-effort and returns the task exit code locally
- Supports pinning placement to a specific client by IP (useful for Windows)
- Supports passing environment variables to the task
- Supports bazz-like `-input`, `-output`, and `-working-directory` flags
- Uploads declared input files before execution and downloads declared output files after a successful run

![](example.png)

## Usage

```bash
./nomm \
  -address http://127.0.0.1:4646 \
  -input ./src/a.txt -input ./src/b.txt \
  -output ./out/result.txt \
  -- \
  sh -lc 'cat a.txt b.txt > ../out/result.txt'

Simple execution without uploaded inputs still works too:

./nomm \
  -address http://127.0.0.1:4646 \
  -datacenter dc1 \
  -environment FOO=bar \
  -environment BAZ=qux \
  -- \
  sh -lc 'echo hello'

Windows client example (`raw_exec` on a Windows Nomad client at a specific IP):
./nomm \
  -address http://127.0.0.1:4646 \
  -node-ip 192.168.3.168 \
  -- \
  cmd /c ver
```

You can also set `NOMAD_ADDR` and `NOMAD_TOKEN` in the environment instead of passing `-address` / `-token`.

## Flags (high level)

Nomad connection:

- `-address`: Nomad address (default `http://127.0.0.1:4646`)
- `-token`: Nomad ACL token (optional)
- `-namespace`: Nomad namespace (optional)
- `-region`: Nomad region (optional)

TLS:

- `-tls-ca`: CA cert to trust (optional)
- `-tls-cert` / `-tls-key`: mTLS client cert + key (optional; must be provided together)
- `-tls-server-name`: TLS SNI override (optional)
- `-tls-insecure-skip-verify`: skip TLS hostname/SAN verification (insecure)

Job shape:

- `-datacenter`: datacenter (repeatable or comma-separated; defaults to `dc1`)
- `-driver`: `raw_exec` (default) or `docker`
- `-docker-image`: required when `-driver=docker`
- `-environment`: env var `KEY=VALUE` (repeatable)
- `-input`: input file path (repeatable or comma-separated)
- `-output`: expected output file path (repeatable or comma-separated)
- `-working-directory`: remote working directory relative to the uploaded input root
- `-cpu`: CPU MHz (default `100`)
- `-memory`: memory MB (default `128`)

Lifecycle:

- `-timeout`: overall timeout for register → alloc completion (default `30m`)
- `-keep-job`: do not deregister job after completion
- `-dry-run`: print computed job info and exit without contacting Nomad

## Notes

- The input/output path model mirrors `bazz`: inputs are uploaded relative to their most common input directory, and outputs are written back relative to their most common output directory.
- Declared `-output` paths are resolved from the staged remote working directory, so `-working-directory subdir -output result.txt` looks for `subdir/result.txt` in the staged workspace and writes back `result.txt` locally.
- The upload/download path is implemented for `raw_exec` and selects a platform-specific wrapper automatically from the target node's OS.
- On Windows `raw_exec` nodes, the generated wrapper uses `cmd.exe` and `certutil`: `cmd.exe` is the command runner, and `certutil -decode` reconstructs uploaded file bytes from embedded Base64 without requiring PowerShell or other extra tools.
- On POSIX `raw_exec` nodes, the generated wrapper uses `/bin/sh`.
- `AllocFS` is used only for reading outputs and logs back from the allocation. The Nomad API exposed by `AllocFS` is read-only, so uploads cannot be performed through it.
