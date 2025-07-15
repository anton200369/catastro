#!/usr/bin/env python3
"""Run numbered scripts sequentially after installing dependencies."""
import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "1.columnas_tablas.py",
    "2.parsear_direcciones.py",
    "3.asignar_codvia.py",
    "4.direcciones_desc.py",
    "5.tablas_resumen.py",
    "6.candidatos_pendientes.py",
]

REQUIREMENTS = "requirements.txt"
SENTINEL = Path(".deps_installed")


def install_deps():
    """Install required packages if not already installed."""
    if SENTINEL.exists():
        return
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", REQUIREMENTS], check=True)
    SENTINEL.touch()


def run_scripts():
    """Execute numbered scripts in order."""
    for script in SCRIPTS:
        subprocess.run([sys.executable, script], check=True)


if __name__ == "__main__":
    install_deps()
    run_scripts()
