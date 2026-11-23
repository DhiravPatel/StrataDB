# Deployment

## systemd (Linux)
```bash
sudo useradd --system --home /var/lib/stratadb --shell /usr/sbin/nologin stratadb
sudo install -d -o stratadb -g stratadb -m 0700 /var/lib/stratadb /var/backups/stratadb
sudo install -m 0755 target/release/strata-server target/release/strata /usr/local/bin/
sudo install -D -m 0640 deployments/examples/strata.toml /etc/stratadb/strata.toml   # edit it
sudo -u stratadb strata-server config --config /etc/stratadb/strata.toml           # validate
sudo install -m 0644 deployments/systemd/stratadb.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now stratadb
sudo -u stratadb strata-server token create --data-dir /var/lib/stratadb --tenant acme --permissions ingest,query
```

## Docker
`deployments/docker/Dockerfile` (non-root UID 10001, `/data` volume, TLS
material mounted at `/etc/stratadb/tls`, built-in healthcheck) and
`deployments/compose/docker-compose.yml` (read-only root FS, all capabilities
dropped, 60 s stop grace period for flushing). **The image build has not been
verified yet** (no Docker daemon on the development machine).

## Operational checklist
- Keep `data_dir` on a local SSD with `fsync` that honours flushes (no network FS).
- Monitor `/api/v1/metrics` (`stratadb_engine_failed`, `stratadb_disk_free_bytes`,
  `stratadb_write_rejections_total`, `stratadb_disk_write_rejections_total`).
- `/api/v1/ready` returns 503 when the engine is in the failed state (restart required).
- Back up to another failure domain (docs/runbooks/backup-restore.md).
- Verify the audit chain periodically: `strata-server audit-verify --data-dir …`.
