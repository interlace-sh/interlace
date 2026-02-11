# SFTP Sync Demo (Phase A)

This project demonstrates the **SFTP → (optional PGP decrypt) → destination sync** pipeline.

It’s designed to be run via the Interlace codebase (same repo), using the new sync subsystem.

## Public SFTP server for quick testing

You can test connectivity against a free public SFTP server (read-only) such as Rebex:

- Host: `test.rebex.net`
- Port: `22`
- Username: `demo`
- Password: `password`

Reference list: [`sftp.net/public-online-sftp-servers`](https://www.sftp.net/public-online-sftp-servers)

Note: Because public servers are read-only and may change structure, you may need to adjust `remote_glob` after listing directories.

## How it works

1. Polls remote SFTP paths using `remote_glob`.
2. Detects changed/new files using `(mtime, size)` and a StateStore-backed manifest.
3. Downloads changed files to a local staging directory.
4. Runs an optional processor chain (e.g., `pgp_decrypt`).
5. Uploads results to destination (filesystem or S3).

## Quick start

### 1) Configure

Edit `config.yaml` and set:
- `sync.jobs[0].remote_glob` to match a real directory on the server
- destination to `filesystem_exports` (default) or `s3_exports`

### 2) Run one sync tick

From repo root:

```bash
cd projects/sftp
python run_sync_once.py
```

This will print a summary like: processed/skipped/errors and a few output URIs.

## PGP (optional)

This demo includes a `pgp_decrypt` processor option implemented with **PGPy** (pure Python).
For large files / streaming decryption, consider adding a `gpg_decrypt` processor later.

