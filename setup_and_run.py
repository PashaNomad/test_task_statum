"""
Этот модуль создает локальное виртуальное окружение и устанавливает
необходимые зависимости из файла requirements.txt. Также он запускает
основной скрипт внутри этого виртуального окружения.
"""

import os
import subprocess
import venv

# Функция для создание локального виртуального окружения
def create_virtualenv(venv_path):
    """
    Создает локальное виртуальное окружение в указанной папке.

    :param venv_path: Путь к папке, где будет создано виртуальное окружение.
    """
    print(f"Создание виртуального окружения в папке '{venv_path}'...")
    venv.create(venv_path, with_pip=True)
    print(f"Виртуальное окружение '{venv_path}' создано.")

# Функция для установки зависимостей из requirements.txt
def install_requirements(venv_python):
    """
    Устанавливает зависимости из файла requirements.txt в указанном виртуальном окружении.

    :param venv_python: Путь к интерпретатору Python в виртуальном окружении.
    """
    print("Установка зависимостей из 'requirements.txt'...")
    subprocess.run([venv_python, "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run([venv_python, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
    print("Все зависимости успешно установлены.")

if __name__ == "__main__":
    # Задаём переменные для локального виртуального окружения
    VENV_DIR = "test_task_venv"

    # Проверка, существует ли виртуальное окружение
    if not os.path.exists(VENV_DIR):
        # Создание виртуального окружения, если оно не существует
        create_virtualenv(VENV_DIR)

    # Определение пути к Python в виртуальном окружении
    if os.name == "nt":
        # Для ОС Windows
        curr_venv_python = os.path.join(VENV_DIR, "Scripts", "python.exe")
    else:
        # Для UNIX-like ОС
        curr_venv_python = os.path.join(VENV_DIR, "bin", "python")

    # Устанавливаем зависимости
    install_requirements(curr_venv_python)

    # Запускаем основной код в виртуальном окружении
    print("Запуск основного скрипта внутри виртуального окружения...")
    subprocess.run([curr_venv_python, "main_script.py"], check=True)
