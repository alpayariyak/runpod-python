'''
PodWorker | modules | lifecycle.py

Performs the following lifecycle operations:
- Shutting down the worker
'''
# pylint: disable=too-few-public-methods

import os
import uuid
import threading
import requests
from requests.adapters import HTTPAdapter, Retry

from .logging import log


class LifecycleManager:
    ''' Lifecycle Manager '''

    def __init__(self):
        '''
        Initialize Lifecycle Manager
        '''
        self.worker_id = os.environ.get('RUNPOD_POD_ID', str(uuid.uuid4()))
        log(f'Worker ID: {self.worker_id}')

        self.job_id = None

        # ------------------------------ Request Session ----------------------------- #
        self.rp_session = requests.Session()

        self.rp_session.headers.update({"Authorization": f"{os.environ.get('RUNPOD_AI_API_KEY')}"})

        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

        # Applies to all https requests made with this session
        self.rp_session.mount('https://', HTTPAdapter(max_retries=retries))

    def heartbeat_ping(self):
        '''
        Pings the heartbeat endpoint
        '''
        webhook_ping = os.environ.get('RUNPOD_WEBHOOK_PING', None)
        ping_interval = int(os.environ.get('RUNPOD_PING_INTERVAL', 10000))
        ping_params = None

        try:
            if self.job_id is not None:
                ping_params = {
                    'job_id': self.job_id,
                }

            if webhook_ping is not None:
                webhook_ping = webhook_ping.replace('$RUNPOD_POD_ID', self.worker_id)

                self.rp_session.get(
                    webhook_ping,
                    params=ping_params,
                    timeout=int(ping_interval/1000)
                )

            log(f'Heartbeat URL: {webhook_ping} Interval: {ping_interval}ms', "DEBUG")
            log(f"Heartbeat Params: {ping_params}", "DEBUG")

        finally:
            heartbeat_thread = threading.Timer(int(ping_interval/1000), self.heartbeat_ping)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()
