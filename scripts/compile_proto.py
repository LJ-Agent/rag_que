"""Proto compilation script. Run after editing que.proto."""
import subprocess
import sys
from pathlib import Path

PROTO_DIR = Path(__file__).parent.parent / "proto"
OUT_DIR = Path(__file__).parent.parent / "src" / "communication" / "grpc_server" / "generated"


def compile_protos():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for proto_file in PROTO_DIR.glob("*.proto"):
        print(f"Compiling {proto_file.name}...")
        subprocess.run(
            [
                sys.executable, "-m", "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={OUT_DIR}",
                f"--grpc_python_out={OUT_DIR}",
                str(proto_file),
            ],
            check=True,
        )
    # Fix imports in generated _pb2.py and _pb2_grpc.py files
    for gen_file in OUT_DIR.glob("*_pb2*.py"):
        content = gen_file.read_text()
        for pb2_file in OUT_DIR.glob("*_pb2.py"):
            if pb2_file.stem != "__init__":
                content = content.replace(
                    f"import {pb2_file.stem} as",
                    f"from communication.grpc_server.generated import {pb2_file.stem} as",
                )
        gen_file.write_text(content)

    (OUT_DIR / "__init__.py").touch()
    print(f"Done. Generated files in {OUT_DIR}")


if __name__ == "__main__":
    compile_protos()
