''' PodWorker | modules | logging.py '''

import os
from dotenv import load_dotenv


env_path = os.getcwd() + '/.env'
load_dotenv(env_path)  # Load environment variables


def log(message, level='INFO'):
    '''
    Log message to stdout
    If set_level is DEBUG or set_level is lower than level, log the message.
    '''
    set_level = os.environ.get('RUNPOD_DEBUG_LEVEL', 'DEBUG').upper()

    if os.environ.get('RUNPOD_DEBUG', 'False') != 'true':
        return

    if set_level == 'ERROR' and level != 'ERROR':
        return

    if set_level == 'WARN' and level not in ['ERROR', 'WARN']:
        return

    if set_level == 'INFO' and level not in ['ERROR', 'WARN', 'INFO']:
        return

    print(f'{level} | {message}')
    return


def log_secret(secret_name, secret, level='INFO'):
    '''
    Censors secrets for logging
    Replaces everything except the first and last characters with *
    '''
    if secret is None:
        secret = 'Could not read environment variable.'
        log(f"{secret_name}: {secret}", 'ERROR')
    else:
        secret = str(secret)
        redacted_secret = secret[0] + '*' * len(secret) + secret[-1]
        log(f"{secret_name}: {redacted_secret}", level)


log('Logging module loaded')

log_secret('RUNPOD_AI_API_KEY', os.environ.get('RUNPOD_AI_API_KEY', None))
log_secret('RUNPOD_WEBHOOK_GET_JOB', os.environ.get('RUNPOD_WEBHOOK_GET_JOB', None))
log_secret('RUNPOD_WEBHOOK_POST_OUTPUT', os.environ.get('RUNPOD_WEBHOOK_POST_OUTPUT', None))
