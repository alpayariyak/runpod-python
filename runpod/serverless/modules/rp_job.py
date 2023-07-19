"""
Job related helpers.
"""

import inspect
from typing import Any, Callable, Dict, Generator, Optional, Union

import os
import json
import traceback
from aiohttp import ClientSession

from runpod.serverless.modules.rp_logger import RunPodLogger
from .worker_state import WORKER_ID, Jobs
from .rp_tips import check_return_size

JOB_GET_URL = str(os.environ.get('RUNPOD_WEBHOOK_GET_JOB')).replace('$ID', WORKER_ID)

log = RunPodLogger()


async def get_job(session: ClientSession, retry=True) -> Optional[Dict[str, Any]]:
    """
    Get the job from the queue.
    Will continue trying to get a job until one is available.

    Inputs:
    - session | The aiohttp session

    Note: Retry True just for ease of, if testing improved this can be removed.
    """
    next_job = None
    job_list = Jobs()

    while next_job is None:
        try:
            job_in_progress = '1' if job_list.get_job_list() else '0'
            async with session.get(JOB_GET_URL + f"&job_in_progress={job_in_progress}") as response: # pylint: disable=line-too-long
                if response.status == 204:
                    log.debug("No content, no job to process.")
                    if not retry:
                        return None
                    continue

                if response.status not in [200, 400]:
                    log.error(f"Failed to get job, status code: {response.status}")
                    if not retry:
                        return None
                    continue

                next_job = await response.json()
                log.debug(f"Received Job | {next_job}")

            # Check if the job is valid
            if next_job.get("id", None) is None:
                log.error("Job has no id, unable to process.")
                next_job = None

            if next_job.get("input", None) is None:
                log.error("Job has no input, unable to process.")
                next_job = None

        except Exception as err:  # pylint: disable=broad-except
            log.error(f"Error while getting job: {err}")

        if next_job is None:
            log.debug("No job available, waiting for the next one.")
            if not retry:
                return None

    log.debug(f"{next_job['id']} | Job Confirmed")
    return next_job


async def run_job(handler: Callable, job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the job using the handler.
    Returns the job output or error.
    """
    log.info(f'{job["id"]} | Started')

    try:
        result = handler(job)
        job_output = await result if inspect.isawaitable(result) else result

        log.debug(f'{job["id"]} | Handler output: {job_output}')

        run_result = {"output": job_output}

        if isinstance(job_output, dict):

            if job_output.get("error", False):
                run_result = {"error": str(job_output["error"])}

            if job_output.get("refresh_worker", False):
                job_output.pop("refresh_worker")
                run_result = {
                    "stopPod": True,
                    "output": job_output
                }

        elif isinstance(job_output, bool):
            run_result = {"output": job_output}

        check_return_size(run_result)  # Checks the size of the return body.
    except Exception as err:    # pylint: disable=broad-except
        error_content = json.dumps(
            {
                "error_type": str(type(err)),
                "error_message": str(err),
                "error_traceback": traceback.format_exc(),
                "host_name": os.environ.get("RUNPOD_POD_HOSTNAME", "unknown"),
                "pod_id": os.environ.get("RUNPOD_POD_ID", "unknown")
            }, indent=4)

        log.error(f'{job["id"]} | Captured Handler Exception')
        log.error(error_content)

        run_result = {"error": error_content}
    finally:
        log.debug(f'{job["id"]} | run_job return: {run_result}')

        return run_result  # pylint: disable=lost-exception


async def run_job_generator(
        handler: Callable,
        job: Dict[str, Any]) -> Generator[Dict[str, Union[str, Any]], None, None]:
    '''
    Run generator job.
    Yields output partials from the generator.
    '''
    try:
        job_output = handler(job)
        for output_partial in job_output:
            yield {"output": output_partial}
    except Exception as err:    # pylint: disable=broad-except
        log.error(f'Error while running job {job["id"]}: {err}')
        yield {"error": f"handler: {str(err)} \ntraceback: {traceback.format_exc()}"}
    finally:
        log.info(f'{job["id"]} | Finished ')

        # return None  # pylint: disable=lost-exception
