"""Shell execution tool."""

import asyncio
import os
import re
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.agent.tools.base import Tool


class ExecTool(Tool):
    """Tool to execute shell commands."""

    # This tool can ask the user to approve particularly dangerous commands.
    supports_approval = True

    def __init__(
        self,
        timeout: int = 60,
        working_dir: str | None = None,
        deny_patterns: list[str] | None = None,
        danger_patterns: list[tuple[str, str]] | None = None,
        allow_patterns: list[str] | None = None,
        restrict_to_workspace: bool = False,
        path_append: str = "",
    ):
        self.timeout = timeout
        self.working_dir = working_dir
        # Catastrophic, never-allowed commands (no legitimate agent use).
        self.deny_patterns = deny_patterns or [
            r"\bdel\s+/[fq]\b",              # del /f, del /q
            r"\brmdir\s+/s\b",               # rmdir /s
            r"(?:^|[;&|]\s*)format\b",       # format (as standalone command only)
            r"\b(mkfs|diskpart)\b",          # disk operations
            r"\bdd\s+if=",                   # dd
            r">\s*/dev/sd",                  # write to disk
            r"\b(shutdown|reboot|poweroff)\b",  # system power
            r":\(\)\s*\{.*\};\s*:",          # fork bomb
        ]
        # Particularly dangerous but sometimes legitimate — require human
        # approval. Each entry is (pattern, reason, block_without_approver):
        #   block_without_approver=True  → if no approver is wired (CLI,
        #     subagents, other channels), the command is blocked (fail closed).
        #     Used for things previously hard-denied (e.g. rm -rf).
        #   block_without_approver=False → if no approver is wired, the command
        #     runs as before (fail open). Used for commands that previously ran
        #     freely everywhere; only the interactive web UI gets to gate them.
        self.danger_patterns = danger_patterns or [
            (r"\brm\s+-[rf]{1,2}\b", "递归/强制删除文件（rm -rf）", True),
            (r"(curl|wget)\b[^|]*\|\s*(sudo\s+)?(ba)?sh\b", "下载并直接执行脚本（curl | sh）", True),
            (r"\bsudo\b", "以管理员权限执行（sudo）", False),
            (r"(?:^|[;&|]\s*)su\b", "切换用户（su）", False),
            (r"\bgit\s+push\b", "推送到远端仓库（git push）", False),
            (r"\bgit\s+reset\s+--hard\b", "丢弃未提交改动（git reset --hard）", False),
            (r"\bgit\s+clean\s+-[a-z]*f", "删除未跟踪文件（git clean -f）", False),
            (r"\bchmod\s+(-[a-z]+\s+)?[0-7]*777\b", "放开全部权限（chmod 777）", False),
            (r"\bchmod\s+-[a-z]*r", "递归修改权限（chmod -R）", False),
            (r"\bchown\s+-[a-z]*r", "递归修改属主（chown -R）", False),
            (r"\b(kill|pkill|killall)\b", "结束进程（kill/pkill）", False),
            (r"\b(apt|apt-get|yum|dnf|brew|pacman)\s+(install|remove|purge|upgrade)\b",
             "安装/卸载系统软件包", False),
            (r">\s*/(etc|usr|bin|boot|sys|lib)\b", "写入系统目录", True),
        ]
        self.allow_patterns = allow_patterns or []
        self.restrict_to_workspace = restrict_to_workspace
        self.path_append = path_append

    @property
    def name(self) -> str:
        return "exec"

    @property
    def description(self) -> str:
        return "Execute a shell command and return its output. Use with caution."

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to execute"
                },
                "working_dir": {
                    "type": "string",
                    "description": "Optional working directory for the command"
                }
            },
            "required": ["command"]
        }
    
    async def execute(self, command: str, working_dir: str | None = None,
                      approval_cb: Any = None, **kwargs: Any) -> str:
        cwd = working_dir or self.working_dir or os.getcwd()
        guard_error = self._guard_command(command, cwd)
        if guard_error:
            return guard_error

        # Particularly dangerous commands require explicit human approval.
        danger = self._danger_reason(command)
        if danger:
            reason, block_without_approver = danger
            if approval_cb is None:
                # No approver available (CLI, subagent, other channels).
                if block_without_approver:
                    return ("Error: This command is flagged as dangerous and no approval "
                            f"channel is available, so it was blocked. Reason: {reason}. "
                            "Choose a safer approach.")
                # Otherwise preserve prior behavior: run it (fail open).
            else:
                try:
                    approved = await approval_cb(command, reason)
                except Exception as e:
                    logger.warning("Approval callback failed for command {!r}: {}", command, e)
                    approved = False
                if not approved:
                    return ("Error: The user declined to approve this command "
                            f"(flagged: {reason}). Do not retry the same command; "
                            "consider a safer alternative or ask the user how to proceed.")

        env = os.environ.copy()
        if self.path_append:
            env["PATH"] = env.get("PATH", "") + os.pathsep + self.path_append

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env,
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                # Wait for the process to fully terminate so pipes are
                # drained and file descriptors are released.
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                return f"Error: Command timed out after {self.timeout} seconds"
            
            output_parts = []
            
            if stdout:
                output_parts.append(stdout.decode("utf-8", errors="replace"))
            
            if stderr:
                stderr_text = stderr.decode("utf-8", errors="replace")
                if stderr_text.strip():
                    output_parts.append(f"STDERR:\n{stderr_text}")
            
            if process.returncode != 0:
                output_parts.append(f"\nExit code: {process.returncode}")
            
            result = "\n".join(output_parts) if output_parts else "(no output)"
            
            # Truncate very long output
            max_len = 10000
            if len(result) > max_len:
                result = result[:max_len] + f"\n... (truncated, {len(result) - max_len} more chars)"
            
            return result
            
        except Exception as e:
            return f"Error executing command: {str(e)}"

    def _guard_command(self, command: str, cwd: str) -> str | None:
        """Best-effort safety guard for potentially destructive commands."""
        cmd = command.strip()
        lower = cmd.lower()

        for pattern in self.deny_patterns:
            if re.search(pattern, lower):
                return "Error: Command blocked by safety guard (dangerous pattern detected)"

        if self.allow_patterns:
            if not any(re.search(p, lower) for p in self.allow_patterns):
                return "Error: Command blocked by safety guard (not in allowlist)"

        if self.restrict_to_workspace:
            if "..\\" in cmd or "../" in cmd:
                return "Error: Command blocked by safety guard (path traversal detected)"

            cwd_path = Path(cwd).resolve()

            for raw in self._extract_absolute_paths(cmd):
                try:
                    p = Path(raw.strip()).resolve()
                except Exception:
                    continue
                if p.is_absolute() and cwd_path not in p.parents and p != cwd_path:
                    return "Error: Command blocked by safety guard (path outside working dir)"

        return None

    def _danger_reason(self, command: str) -> tuple[str, bool] | None:
        """If the command is particularly dangerous, return (reason, block_without_approver)."""
        lower = command.strip().lower()
        for pattern, reason, block_without_approver in self.danger_patterns:
            if re.search(pattern, lower):
                return reason, block_without_approver
        return None

    @staticmethod
    def _extract_absolute_paths(command: str) -> list[str]:
        win_paths = re.findall(r"[A-Za-z]:\\[^\s\"'|><;]+", command)   # Windows: C:\...
        posix_paths = re.findall(r"(?:^|[\s|>])(/[^\s\"'>]+)", command) # POSIX: /absolute only
        return win_paths + posix_paths
