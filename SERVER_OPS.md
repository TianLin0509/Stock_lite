# Server Ops Memo

This file records the current deployment shape for the Alibaba Cloud server used by the WeChat backend.
It intentionally excludes passwords, private keys, and other raw secrets.

## Server

- Host: `8.130.158.231`
- OS: `Windows Server 2022`
- SSH user: `Administrator`
- SSH auth: local key-based login is configured
- Local key path used by Codex on this machine: `C:\Users\lintian\.ssh\codex_aliyun`

## App Layout On Server

- Deploy root: `C:\StockLite\app`
- Incoming bundle dir: `C:\StockLite\incoming`
- Virtualenv python: `C:\StockLite\app\.venv\Scripts\python.exe`
- Logs:
  - `C:\StockLite\app\logs\uvicorn.stdout.log`
  - `C:\StockLite\app\logs\uvicorn.stderr.log`
- PID file: `C:\StockLite\app\uvicorn.pid`

## Public Endpoints

- WeChat callback: `http://8.130.158.231/wechat`
- Report page: `http://8.130.158.231/report/{report_id}`

## Deployment Notes

- App is deployed by uploading `deploy_bundle.zip` to `C:\StockLite\incoming\deploy_bundle.zip`
- Remote deploy script:
  - `C:\StockLite\incoming\deploy_bundle.ps1`
- Remote start script:
  - `C:\StockLite\incoming\start_backend.ps1`

## Local Helper Scripts

- `remote_exec.py`
- `remote_upload.py`
- `build_deploy_bundle.py`
- `remote_scripts\deploy_bundle.ps1`
- `remote_scripts\start_backend.ps1`
- `remote_scripts\inspect_backend.ps1`
- `remote_scripts\check_http.ps1`

## Current Runtime Shape

- FastAPI app entry: `main.py`
- Service binds on: `0.0.0.0:80`
- Windows firewall rule for TCP 80 is added by start script
- Alibaba Cloud security group must keep TCP 80 open

## Important Code Adjustments Made For Server

- `main.py`
  - token balance endpoint uses lazy import so optional cloud SDK failures do not block WeChat service startup
- `utils\app_config.py`
  - supports reading `.streamlit\secrets.toml` directly without Streamlit runtime
- `requirements.deploy.txt`
  - server deployment dependency set

## Standard Remote Workflow

1. Rebuild bundle locally:
   - `python build_deploy_bundle.py`
2. Upload bundle:
   - `python remote_upload.py --host 8.130.158.231 --user Administrator --key C:\Users\lintian\.ssh\codex_aliyun --local C:\Users\lintian\Stock_lite\deploy_bundle.zip --remote C:/StockLite/incoming/deploy_bundle.zip`
3. Upload changed scripts if needed:
   - `deploy_bundle.ps1`
   - `start_backend.ps1`
4. Run remote deploy:
   - `python remote_exec.py --host 8.130.158.231 --user Administrator --key C:\Users\lintian\.ssh\codex_aliyun --command "powershell -NoProfile -ExecutionPolicy Bypass -File C:\StockLite\incoming\deploy_bundle.ps1"`
5. Start backend:
   - `python remote_exec.py --host 8.130.158.231 --user Administrator --key C:\Users\lintian\.ssh\codex_aliyun --command "powershell -NoProfile -ExecutionPolicy Bypass -File C:\StockLite\incoming\start_backend.ps1"`
6. Inspect backend:
   - `python remote_exec.py --host 8.130.158.231 --user Administrator --key C:\Users\lintian\.ssh\codex_aliyun --script-file C:\Users\lintian\Stock_lite\remote_scripts\inspect_backend.ps1`

## Validation Signals

- `http://8.130.158.231/report/test` returns `404` when app is healthy
- `http://8.130.158.231/wechat?...` returns a response from FastAPI
- WeChat official platform callback verification has already succeeded once
- Real message roundtrip has already succeeded once

## Future Upgrade Path

- Add domain
- Add HTTPS on `443`
- Tighten security group rules for `22` and `3389`
- Rotate server password because it was exposed during setup

## Lessons Learned 2026-03-25

- For any WeChat feature, treat the server as the source of truth. Do not assume a local code change is live until the remote `main.py` has been uploaded, the backend restarted, and the public endpoints have been verified.
- After every server-side change, validate the real chain in this order:
  1. `http://8.130.158.231/report/test` should return `404`
  2. `http://8.130.158.231/wechat` should return `422` when called without query params
  3. `C:\StockLite\app\logs\wechat_server.log` should show the new request hitting the expected branch
- Do not close a task after only seeing the immediate XML reply. For WeChat background tasks, also verify the async phase by checking for `run_kline_prediction_analysis success` or `failed` in `wechat_server.log`.
- Be careful with how the Windows backend is started remotely. `Start-Process` launched from a transient SSH session may not survive the session. Prefer a detached launcher or scheduled-task-based start that is known to stay alive after the remote session exits.
- When testing Chinese WeChat commands from this workstation, do not trust inline PowerShell here-docs for payloads. They can corrupt UTF-8 text into `???`. Use a saved UTF-8 `.py` script for message simulation.
- For all-market K-line prediction, design for server memory first. The full research dataset is about 1,000,000 rows, so training must avoid large `.copy()` operations on the full frame. Use column-pruning and bounded sampling before fitting.
- Before deploying a research-heavy feature, profile one real server-side request end to end. In this incident, the report build took around 40-90 seconds depending on path, which is acceptable for background processing but not for synchronous assumptions.
- When a user says the main code runs on the server, default to server-first debugging and deployment. Do not imply the feature is fixed until the remote environment has been proven with a public request.
