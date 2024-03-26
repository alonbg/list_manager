import subprocess
import time
import atexit
from threading import Thread, Event
import shlex
from addict import Dict
from .. import log, Utils

default_ssh_config = Dict(
    {
        "ssh_host": None,
        "ssh_username": "root",
        "ssh_pkey": None,
        "local_bind_address": ("127.0.0.1", 44444),
        "remote_bind_address": ("127.0.0.1", 55555),
    }
)


class SSHUDPTunnel:
    def __init__(
        self,
        local_port,
        remote_port,
        remote_host="localhost",
        ssh_config=default_ssh_config,
    ):
        self.ssh_config = ssh_config
        self.local_port = local_port
        self.remote_port = remote_port
        self.remote_host = remote_host
        self.local_socat_command = self.get_local_command()
        self.remote_socat_command = self.get_remote_command()
        self.local_socat_process = None
        self.remote_socat_process = None
        self.monitor_thread = None
        self.stop_event = Event()
        atexit.register(self.stop)

    def get_local_command(self):
        err_log = Utils.with_root("local_socat_err.log")
        return f"socat -lf {err_log} udp4-listen:{self.local_port},reuseaddr,fork tcp:localhost:{self.ssh_config.local_bind_address}"

    def get_remote_command(self):
        socat_command = f"socat tcp4-listen:{self.ssh_config.remote_bind_address},reuseaddr,fork UDP:localhost:{self.remote_port}"
        # Wrap the remote command with bash -c and an additional layer of quoting for the remote shell
        remote_bash_command = f"bash -O huponexit -c {shlex.quote(socat_command)}"

        # Construct the full SSH command
        ssh_command = [
            "ssh",
            "-t",  # Allocate a pseudo-terminal
            "-i",
            shlex.quote(self.ssh_config["ssh_pkey"]),
            "-L",
            f"{self.ssh_config.local_bind_address}:localhost:{self.ssh_config.remote_bind_address}",
            f"{shlex.quote(self.ssh_config['ssh_username'])}@{shlex.quote(self.ssh_config['ssh_host'])}",
            f"{remote_bash_command}",
        ]

        return shlex.join(ssh_command)

    def _start_local_socat(self):
        self.local_socat_process = subprocess.Popen(
            self.local_socat_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give a moment for socat to initialize
        time.sleep(0.5)  # Adjust the sleep time if necessary

    def _stop_local_socat(self):
        if self.local_socat_process:
            self.local_socat_process.terminate()
            self.local_socat_process.wait()
            self.local_socat_process = None

    def _watch_local_socat(self):
        if self.local_socat_process and self.local_socat_process.poll() is not None:
            log.error(
                f"Local socat process has exited unexpectedly with exit code {self.local_socat_process.returncode}."
            )
            # Read any output if available
            stdout, stderr = self.local_socat_process.communicate()
            if stdout:
                log.info(f"Local socat stdout: {stdout.decode().strip()}")
            if stderr:
                log.info(f"Local socat stderr: {stderr.decode().strip()}")

            log.info("Attempting to restart Local socat process ...")
            self._stop_local_socat()
            self._start_local_socat()

    def _start_remote_socat(self):
        self.remote_socat_process = subprocess.Popen(
            self.remote_socat_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give a moment for socat to initialize
        time.sleep(2)  # Adjust the sleep time if necessary

    def _stop_remote_socat(self):
        if self.remote_socat_process and self.remote_socat_process.poll() is not None:
            self.remote_socat_process.terminate()
            self.remote_socat_process.wait()
            self.remote_socat_process = None

    def _watch_remote_socat(self):
        if self.remote_socat_process and self.remote_socat_process.poll() is not None:
            log.error(
                f"Remote socat process has exited unexpectedly with exit code {self.remote_socat_process.returncode}."
            )
            # Read any output if available
            stdout, stderr = self.remote_socat_process.communicate()
            if stdout:
                log.info(f"Remote socat stdout: {stdout.strip()}")
            if stderr:
                log.info(f"Remote socat stderr: {stderr.strip()}")

            log.info("Attempting to restart Remote socat process ...")
            self._stop_remote_socat()
            self._start_remote_socat()

    def _monitor_tunnel(self):
        while not self.stop_event.is_set():
            # Monitor the local socat process
            self._watch_local_socat()
            # Check if the local socat process has exited
            self._watch_remote_socat()
            # Sleep for some time before checking again
            time.sleep(10)

    def start(self):
        log.info("Starting tunnel...")
        self.stop_event.clear()
        # Start the remote socat
        self._start_remote_socat()
        # Start the local socat
        self._start_local_socat()
        # Start monitoring the tunnel in a separate thread
        self.monitor_thread = Thread(target=self._monitor_tunnel)
        self.monitor_thread.start()
        log.info("Tunnel started and being monitored.")

    def stop(self):
        log.info("Stopping tunnel...")
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.stop_event.set()  # Signal the monitor thread to stop
            self.monitor_thread.join()  # Wait for the monitor thread to finish
        # Stop the local socat process
        self._stop_local_socat()
        # Stop the remote socat process
        self._stop_remote_socat()
        log.info("Tunnel stopped.")
