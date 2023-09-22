'''
runpod | serverless | rp_scale.py
Provides the functionality for scaling the runpod serverless worker.
'''

import asyncio
import typing

from runpod.serverless.modules.rp_logger import RunPodLogger
from .rp_job import get_job
from .worker_state import Jobs

log = RunPodLogger()
job_list = Jobs()


class JobScaler():
    """
    A class for automatically retrieving new jobs from the server and processing them concurrently.

    Methods:
        get_jobs() -> List[Dict]:
            Retrieves multiple jobs from the server in parallel using concurrent get requests.

        upscale_rate() -> None:
            Upscales the job retrieval rate by increasing the number of concurrent get requests.

        downscale_rate() -> None:
            Downscales the job retrieval rate by reducing the number of concurrent get requests.

        rescale_request_rate() -> None:
            Rescales the job retrieval rate based on the current level of utilization in the worker.

    Usage example:
        job_scaler = JobScaler(config)

        # Retrieving multiple jobs in parallel
        jobs_list = job_scaler.get_jobs(session)

        # Upscaling the rate for faster job retrieval
        job_scaler.upscale_rate()

        # Downscaling the rate for more conservative job retrieval
        job_scaler.downscale_rate()

        # Rescaling based on the queue, availability, and other metrics
        job_scaler.rescale_request_rate()
    """

    # Scaling Constants
    CONCURRENCY_ADDITIVE_FACTOR = 1
    MAX_CONCURRENT_REQUESTS = 1000
    MIN_CONCURRENT_REQUESTS = 1
    SLEEP_INTERVAL_SEC = 1
    QUEUE_AVAILABILITY_THRESHOLD = 0.70

    def __init__(self, max_concurrency: typing.Any):
        self.background_get_job_tasks = set()
        self.max_concurrency = max_concurrency
        self._is_alive = True
        self.job_history = []

        # Let's initiate the number of concurrent job requests around the max concurrency.
        if self.max_concurrency:
            self.num_concurrent_get_job_requests = self.max_concurrency()
        else:
            self.num_concurrent_get_job_requests = JobScaler.MIN_CONCURRENT_REQUESTS

    def is_alive(self):
        """
        Return whether the worker is alive or not.
        """
        return self._is_alive

    def kill_worker(self):
        """
        Whether to kill the worker.
        """
        self._is_alive = False

    def track_task(self, task):
        """
        Keep track of the task to avoid python garbage collection of the coroutine.
        """
        self.background_get_job_tasks.add(task)
        task.add_done_callback(self.background_get_job_tasks.discard)

    async def get_jobs(self, session):
        """
        Retrieve multiple jobs from the server in parallel using concurrent requests.

        Returns:
            List[Any]: A list of job data retrieved from the server.
        """
        while True:
            # Finish if the job_scale is not alive
            if not self.is_alive():
                break

            # Use parallel processing whenever possible
            use_parallel_processing = self.max_concurrency and job_list.get_job_list() is not None

            if use_parallel_processing:
                # Prepare the 'get_job' tasks for parallel execution.
                tasks = [
                    asyncio.create_task(
                        get_job(session, force_in_progress=True, retry=False)
                    )
                    for _ in range(self.num_concurrent_get_job_requests)
                ]

                # Wait for all the 'get_job' tasks, which are running in parallel, to be completed.
                for job_future in asyncio.as_completed(tasks):
                    job = await job_future
                    self.job_history.append(1 if job else 0)
                    if job:
                        yield job
            else:
                for _ in range(self.num_concurrent_get_job_requests):
                    job = await get_job(session, retry=False)
                    if job:
                        yield job

            # During the single processing scenario, wait for the single job to finish processing.
            if self.max_concurrency is None:
                # Create a copy of the background job tasks list to keep references to the tasks.
                job_tasks_copy = self.background_get_job_tasks.copy()
                if job_tasks_copy:
                    # Wait for the job tasks to finish processing before continuing.
                    await asyncio.wait(job_tasks_copy)

                # Exit the loop after processing a single job (since the handler is fully utilized).
                break

            # We retrieve num_concurrent_get_job_requests jobs per second.
            await asyncio.sleep(JobScaler.SLEEP_INTERVAL_SEC)

            # Rescale the retrieval rate appropriately.
            self.rescale_request_rate()

            # Show logs
            log.info(
                f"Concurrent Get Jobs | The number of concurrent get_jobs is "
                f"{self.num_concurrent_get_job_requests}."
                f" Parallel processing = {use_parallel_processing}"
            )

    def upscale_rate(self) -> None:
        """
        Upscale the job retrieval rate by increasing the number of concurrent requests.

        This method increases the number of concurrent requests to the server,
        effectively retrieving more jobs per unit of time.
        """
        self.num_concurrent_get_job_requests = min(
            self.num_concurrent_get_job_requests +
            JobScaler.CONCURRENCY_ADDITIVE_FACTOR,
            JobScaler.MAX_CONCURRENT_REQUESTS
        )

    def downscale_rate(self) -> None:
        """
        Downscale the job retrieval rate by reducing the number of concurrent requests.

        This method decreases the number of concurrent requests to the server,
        effectively retrieving fewer jobs per unit of time.
        """
        self.num_concurrent_get_job_requests = int(max(
            self.num_concurrent_get_job_requests - JobScaler.CONCURRENCY_ADDITIVE_FACTOR,
            JobScaler.MIN_CONCURRENT_REQUESTS
        ))

    def rescale_request_rate(self) -> None:
        """
        Scale up or down the rate at which we are handling jobs from SLS.
        """
        # Compute the availability ratio of the job queue.
        availability_ratio = sum(
            self.job_history) / len(self.job_history) if len(self.job_history) > 0 else None

        # Compute the current level of concurrency inside of the worker
        current_concurrency = len(job_list.jobs)

        # If our worker is fully utilized, reduce the job query rate.
        if availability_ratio is not None and availability_ratio < JobScaler.QUEUE_AVAILABILITY_THRESHOLD:
            log.debug(
                "Reducing job query rate due to low queue availability.")

            self.downscale_rate()
        elif current_concurrency > self.max_concurrency():
            log.debug("Reducing job query rate due to full worker utilization.")
            self.downscale_rate()
        elif current_concurrency < self.max_concurrency():
            log.debug(
                "Increasing job query rate due to worker under-utilization.")
            self.upscale_rate()
        else:
            # Keep in stasis
            pass

        # Clear the job history
        self.job_history.clear()
