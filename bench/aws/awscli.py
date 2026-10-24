"""Small helpers over the AWS CLI and Terraform for bench/aws/run.py.

Everything goes through the `aws` CLI with the profile in AWS_PROFILE (default
audit-trail-bench) and JSON output, so the scripts need nothing beyond the CLI and the
Python standard library on the machine that drives the run.
"""
import json
import os
import shlex
import subprocess
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROFILE = os.environ.get("AWS_PROFILE", "audit-trail-bench")


def aws(*args, region=None, parse=True, check=True):
    cmd = ["aws", "--profile", PROFILE, "--output", "json"]
    if region:
        cmd += ["--region", region]
    cmd += [str(a) for a in args]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        if check:
            raise RuntimeError("aws %s failed: %s" % (" ".join(cmd[5:8]), proc.stderr.strip()))
        return None
    out = proc.stdout.strip()
    return json.loads(out) if parse and out else out


def aws_status(*args, region=None):
    """(exit code, stdout, stderr) of one CLI call, for callers that inspect the error."""
    cmd = ["aws", "--profile", PROFILE, "--output", "json"] + (["--region", region] if region else []) + [str(a) for a in args]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def tf_outputs(root):
    proc = subprocess.run(["terraform", "-chdir=" + os.path.join(ROOT, "infra", root), "output", "-json"],
                          stdout=subprocess.PIPE, text=True, check=True)
    return {k: v["value"] for k, v in json.loads(proc.stdout).items()}


class Runner:
    """Shell commands on the runner instance through SSM Run Command."""

    def __init__(self, instance_id, region, log):
        self.instance_id = instance_id
        self.region = region
        self.log = log

    def run(self, script, timeout_s=3600, quiet=False):
        """Runs a bash script as ec2-user and returns its stdout; raises on a non-zero exit."""
        env = "export AWS_REGION=%s AWS_DEFAULT_REGION=%s\n" % (self.region, self.region)
        wrapped = "sudo -u ec2-user -H bash -lc %s" % shlex.quote("set -euo pipefail\n" + env + script)
        params = json.dumps({"commands": [wrapped], "executionTimeout": [str(timeout_s)]})
        cmd = aws("ssm", "send-command", "--instance-ids", self.instance_id, "--document-name", "AWS-RunShellScript",
                  "--parameters", params, "--timeout-seconds", 600, region=self.region)
        cid = cmd["Command"]["CommandId"]
        deadline = time.time() + timeout_s + 120
        while True:
            time.sleep(2)
            inv = aws("ssm", "get-command-invocation", "--command-id", cid, "--instance-id", self.instance_id,
                      region=self.region, check=False)
            if inv and inv["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
                break
            if time.time() > deadline:
                raise RuntimeError("SSM command %s did not finish" % cid)
        out, err = inv.get("StandardOutputContent", ""), inv.get("StandardErrorContent", "")
        if inv["Status"] != "Success":
            raise RuntimeError("runner command failed (%s, exit %s):\n%s\n%s" % (
                inv["Status"], inv.get("ResponseCode"), out[-3000:], err[-3000:]))
        if not quiet and out.strip():
            for line in out.strip().splitlines()[-15:]:
                self.log("    runner: " + line[:300])
        return out

    def start_job(self, name, script):
        """Starts a long command as a transient systemd unit, output in /data/logs/<name>.log."""
        unit = "atb-" + name
        body = "set -euo pipefail\ncd /data/code/repo\n" + script
        self.run("sudo systemctl reset-failed %s 2>/dev/null || true\n"
                 "sudo systemd-run --unit=%s --uid=ec2-user --gid=ec2-user --setenv=HOME=/home/ec2-user "
                 "--setenv=AWS_REGION=%s --setenv=AWS_DEFAULT_REGION=%s "
                 "--property=StandardOutput=append:/data/logs/%s.log --property=StandardError=append:/data/logs/%s.log "
                 "/bin/bash -lc %s" % (unit, unit, self.region, self.region, name, name, shlex.quote(body)), quiet=True)
        self.log("  started %s on the runner" % unit)

    def wait_job(self, name, poll_s=60, timeout_s=8 * 3600):
        unit = "atb-" + name
        deadline = time.time() + timeout_s
        while True:
            state = self.run("echo $(systemctl show %s -p ActiveState --value) $(systemctl show %s -p Result --value); "
                             "tail -n 2 /data/logs/%s.log || true" % (unit, unit, name), quiet=True)
            first = state.splitlines()[0].split() if state.strip() else []
            if first and first[0] in ("inactive", "failed"):
                if first[0] == "failed" or (len(first) > 1 and first[1] != "success"):
                    tail = self.run("tail -n 60 /data/logs/%s.log" % name, quiet=True)
                    raise RuntimeError("runner job %s failed:\n%s" % (name, tail))
                self.log("  %s finished" % unit)
                return
            if time.time() > deadline:
                raise RuntimeError("runner job %s still running after %d s" % (name, timeout_s))
            self.log("  %s running: %s" % (unit, " | ".join(state.strip().splitlines()[1:])[-200:]))
            time.sleep(poll_s)
