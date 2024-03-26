import subprocess
import time
import atexit
import shlex
import os
from pathlib import Path
from threading import Thread, Event
from addict import Dict
import sshtunnel
import requests
from .. import log, Utils


default_ssh_config = Dict(
    {
        "ssh_host": None,
        "ssh_username": "root",
        "ssh_pkey": None,
        "remote_bind_address": ("0.0.0.0", 8888),
        "local_bind_address": ("127.0.0.1", 8888),
    }
)

class WireGuardConfig:
    def __init__(self, ssh_config=default_ssh_config):
        self.ssh_config = ssh_config
        local_bind_address = ssh_config.local_bind_address
        self.wg_api = f"http://{local_bind_address[0]}:{local_bind_address[1]}"

    def config(self, wg_conf: Path, subnet_id=0, dns=1, replace=dict()):
        with sshtunnel.open_tunnel(**(self.ssh_config)) as tunnel:
            response = requests.get(f"{self.wg_api}/genpeer?grp={subnet_id}&dns={dns}")
            response.raise_for_status()
            with wg_conf.open(mode="w") as fp:
                fp.write("\n".join([l for l in self._replace(response.text, replace)]))

    @staticmethod
    def _replace(conf, rep):
        for l in conf.splitlines():
            parts = l.split("=", 1)
            if len(parts) != 2:
                yield l
                continue
            for k, v in rep.items():
                if k == parts[0].strip():
                    l = f"{parts[0]}= {str(v)}"
                    break
            yield l


class WireGuardManager():
    instance = False
    wireguard_dir = Utils.get_create_dir("wireguard")
    wireguard_go = wireguard_dir.joinpath("wireguard-go/wireguard-go")
    wg_conf = wireguard_dir.joinpath("wg0.conf")
    running = Event()
    configurator = None
    run_thread = None

    def __init__(self, **kwargs):
        if self.instance:
            raise ValueError("WireGuardManager is a singleton")
        
        self.run_thread = Thread(target=self.thread_loop)
        self.running = Event()
        self.configurator = WireGuardConfig(**kwargs)
        atexit.register(self.stop)
        self.instance = True

    def thread_loop(self):
        returncode = self.up()
        if returncode != 0:
            log.error("bailing, wireguard startup returned non-zero code:", returncode)
            return

        cmd = shlex.join(["sudo", "wg", "show"])  # self.wg_conf.stem
        # Loop to check the tunnel status
        while self.running.is_set():
            # Check if the WireGuard interface is up
            result = subprocess.run(cmd, capture_output=True, shell=True)
            if result.returncode != 0:
                # If the interface is not up, break the loop and optionally take action
                log.info("WireGuard interface is down. Exiting thread.")
                break
            time.sleep(5)
        self.down()

    def up(self):
        log.info("Starting wireguard-go ...")
        env = os.environ
        env["WG_QUICK_USERSPACE_IMPLEMENTATION"] = str(self.wireguard_go)
        env["ENV_WG_PROCESS_FOREGROUND"] = str(1)
        # env['LOG_LEVEL'] = 'debug'
        cmd = shlex.join(["sudo", "-E", "wg-quick", "up", str(self.wg_conf)])
        result = subprocess.run(
            cmd,
            env=env,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait a bit for the tunnel to come up
        time.sleep(2)
        return result.returncode

    def down(self):
        log.info("Stopping wireguard-go ...")
        cmd = shlex.join(["sudo", "-E", "wg-quick", "down"])
        result = subprocess.run(
            cmd,
            timeout=60,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return result.returncode

    def single_shot(self, target, config=True, *args, **kwargs):
        if config:
            self.configurator.config(self.wg_conf, **kwargs)
        returncode = self.up()
        if returncode != 0:
            log.error("bailing, wireguard startup returned non-zero code:", returncode)
            return
        target(*args)
        self.down()

    def start(self, config=False, **kwargs):
        if config:
            self.configurator.config(self.wg_conf, **kwargs)
        self.running.set()
        self.run_thread.start()
        log.info("wireguard started")

    def stop(self):
        if self.run_thread.is_alive():
            self.running.clear()  # Signal the monitor thread to stop
            self.run_thread.join()  # Wait for the monitor thread to finish
        log.info("wireguard stopped.")
