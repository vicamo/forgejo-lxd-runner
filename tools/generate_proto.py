"""Generate package-local bindings with grpcio-tools or distro protoc."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--system-protoc",
        action="store_true",
        help="Use protoc and grpc_python_plugin from the distribution.",
    )
    args = parser.parse_args()
    source = Path("proto")
    output = Path("src/forgejo_lxd_runner/proto")
    files = sorted(source.rglob("*.proto"))
    if not files:
        parser.error(f"no .proto files under {source}")
    for child in output.iterdir() if output.exists() else []:
        if child.is_dir():
            shutil.rmtree(child)
    output.mkdir(parents=True, exist_ok=True)
    (output / "__init__.py").touch()
    command = [sys.executable, "-m", "grpc_tools.protoc"]
    if args.system_protoc:
        compiler = shutil.which("protoc")
        plugin = shutil.which("grpc_python_plugin")
        if not compiler or not plugin:
            parser.error("system generation requires protoc and grpc_python_plugin")
        command = [compiler, "-I/usr/include", f"--plugin=protoc-gen-grpc_python={plugin}"]
    subprocess.run(
        [
            *command,
            f"-I{source}",
            f"--python_out={output}",
            f"--pyi_out={output}",
            f"--grpc_python_out={output}",
            *map(str, files),
        ],
        check=True,
    )
    for directory in output.rglob("*"):
        if directory.is_dir():
            (directory / "__init__.py").touch()
    prefix = ".".join(output.relative_to("src").parts)
    for module in output.rglob("*_pb2*.py"):
        text = module.read_text().replace("from plugin.v1alpha ", f"from {prefix}.plugin.v1alpha ")
        text = text.replace("import plugin.v1alpha.", f"import {prefix}.plugin.v1alpha.")
        module.write_text(text)


if __name__ == "__main__":
    main()
