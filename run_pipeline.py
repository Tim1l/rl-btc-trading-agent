import subprocess
import logging
import time
import asyncio
from datetime import datetime

# Настройка логирования с кодировкой UTF-8
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    encoding='utf-8'  # Указываем кодировку UTF-8 для файла логов
)

# Путь к интерпретатору Python из виртуального окружения
PYTHON_EXECUTABLE = r"c:\Users\Administrator\Desktop\rl_agent\venv\Scripts\python.exe"

def run_script_sequential(script_name):
    """Запускает указанный скрипт последовательно и логирует результат."""
    logging.info(f"Starting {script_name}...")
    start_time = time.time()
    try:
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_name],
            check=True,
            capture_output=True,
            encoding='utf-8',
            errors='replace'
        )
        elapsed_time = time.time() - start_time
        logging.info(f"{script_name} completed successfully in {elapsed_time:.2f} seconds")
        logging.info(f"Output: {result.stdout}")
        if result.stderr:
            logging.warning(f"Errors/Warnings: {result.stderr}")
    except subprocess.CalledProcessError as e:
        elapsed_time = time.time() - start_time
        logging.error(f"{script_name} failed after {elapsed_time:.2f} seconds")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"stdout: {e.stdout}")
        logging.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        elapsed_time = time.time() - start_time
        logging.error(f"{script_name} failed after {elapsed_time:.2f} seconds with unexpected error")
        logging.error(f"Error: {e}")
        raise

async def run_script_parallel(script_name):
    """Запускает указанный скрипт асинхронно и логирует результат."""
    logging.info(f"Starting {script_name}...")
    start_time = time.time()
    try:
        process = await asyncio.create_subprocess_exec(
            PYTHON_EXECUTABLE, script_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        elapsed_time = time.time() - start_time
        stdout_decoded = stdout.decode('utf-8', errors='replace')
        stderr_decoded = stderr.decode('utf-8', errors='replace')
        if process.returncode == 0:
            logging.info(f"{script_name} completed successfully in {elapsed_time:.2f} seconds")
            logging.info(f"Output: {stdout_decoded}")
            if stderr_decoded:
                logging.warning(f"Errors/Warnings: {stderr_decoded}")
        else:
            raise subprocess.CalledProcessError(process.returncode, script_name, stdout_decoded, stderr_decoded)
    except subprocess.CalledProcessError as e:
        elapsed_time = time.time() - start_time
        logging.error(f"{script_name} failed after {elapsed_time:.2f} seconds")
        logging.error(f"Error: {e}")
        logging.error(f"Output: {e.output}")
        raise

async def run_parallel_scripts(scripts):
    """Запускает список скриптов параллельно."""
    tasks = [run_script_parallel(script) for script in scripts]
    await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    # Список скриптов для последовательного выполнения
    sequential_scripts = [
        "get_last_candles.py",
        "process_data.py",
        "get_action.py"
    ]

    # Список скриптов для параллельного выполнения
    parallel_scripts = [
        "trade_mt5.py",
        "trade_on_bybit.py"
    ]

    # Скрипты, которые снова выполняются последовательно после параллельных
    final_scripts = [
        "move_logs.py"
    ]

    # Выполняем последовательные скрипты до параллельных
    for script in sequential_scripts:
        run_script_sequential(script)

    # Выполняем параллельные скрипты
    await run_parallel_scripts(parallel_scripts)

    # Выполняем оставшиеся последовательные скрипты
    for script in final_scripts:
        run_script_sequential(script)

if __name__ == "__main__":
    try:
        asyncio.run(main())
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")