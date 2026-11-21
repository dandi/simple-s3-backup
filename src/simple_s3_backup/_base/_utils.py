import datetime
import functools
import subprocess


def _deploy_subprocess(
    *,
    command: str | list[str],
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
    capture_output: bool = True,
) -> str | None:
    """
    Run `command` in a shell and return its stdout.

    With `capture_output=False` its stdout and stderr go where this process's go, as it writes them, and
    nothing is returned; a SLURM task then keeps them in its log even when it is killed mid-command.
    """
    error_message = error_message or "An error occurred while executing the command."

    result = subprocess.run(
        args=command,
        env=environment_variables,
        shell=True,
        capture_output=capture_output,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0 and ignore_errors is False:
        message = (
            f"\n\nError code {result.returncode}\n"
            f"{error_message}\n\n"
            f"stdout: {result.stdout if capture_output else '(above)'}\n\n"
            f"stderr: {result.stderr if capture_output else '(above)'}\n\n"
        )
        raise RuntimeError(message)
    if (result.returncode != 0 and ignore_errors is True) or capture_output is False:
        return None

    return result.stdout


@functools.lru_cache
def _get_today() -> str:
    today = datetime.date.today().isoformat()
    return today
