"""Build a minimal native PyLucene 9.12 binding from verified Apache artifacts.

Run with: uv run python scripts/install_pylucene.py
Cached wheels avoid rebuilding after uv sync. No system Java or Python is changed.
"""

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".cache/pylucene-build"
VERSION = "9.12.0"
SOURCE_SHA256 = "5d7223e87883e64ec0cd3e62a983a4bf50dce68c997081c8989e233b90cee115"
MODULES = ("core", "analysis-common", "queryparser", "queries", "sandbox", "facet")


def digest(path, algorithm="sha256"):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


def download(url, path, checksum, algorithm="sha256"):
    if path.exists() and digest(path, algorithm) == checksum:
        return
    temporary = path.with_suffix(path.suffix + ".part")
    print(f"Downloading {url}", flush=True)
    with requests.get(url, stream=True, timeout=(20, 90)) as response:
        response.raise_for_status()
        with temporary.open("wb") as stream:
            for chunk in response.iter_content(1024 * 1024):
                stream.write(chunk)
    if digest(temporary, algorithm) != checksum:
        raise RuntimeError(f"Publisher checksum mismatch: {path.name}")
    temporary.replace(path)


def run(args, *, cwd=ROOT, env=None):
    print("$ " + " ".join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), cwd=cwd, env=env, check=True)


def java_home():
    configured = os.environ.get("JAVA_HOME")
    saved = CACHE / "java-home.txt"
    if configured and (Path(configured) / "include/jni.h").exists():
        return Path(configured).resolve()
    if saved.exists() and (Path(saved.read_text().strip()) / "include/jni.h").exists():
        return Path(saved.read_text().strip())
    if sys.platform != "darwin" or platform.machine() != "arm64":
        raise RuntimeError(
            "Set JAVA_HOME to a full JDK 17 (including JNI headers), then rerun."
        )
    response = requests.get(
        "https://api.adoptium.net/v3/assets/latest/17/hotspot",
        params={
            "architecture": "aarch64",
            "image_type": "jdk",
            "os": "mac",
            "vendor": "eclipse",
        },
        timeout=30,
    )
    response.raise_for_status()
    package = response.json()[0]["binary"]["package"]
    (CACHE / "temurin-package.json").write_text(json.dumps(package, indent=2) + "\n")
    target = CACHE / package["name"]
    download(package["link"], target, package["checksum"])
    with tarfile.open(target) as archive:
        archive.extractall(CACHE / "jdk", filter="data")
    found = next((CACHE / "jdk").glob("*/Contents/Home"))
    saved.write_text(str(found))
    return found


def main():
    if shutil.which("uv") is None or shutil.which("c++") is None:
        raise RuntimeError(
            "Install uv and a C++ compiler (Xcode command-line tools on macOS)."
        )
    CACHE.mkdir(parents=True, exist_ok=True)
    wheels = CACHE / f"wheels-{sys.implementation.cache_tag}-{platform.machine()}"
    wheels.mkdir(exist_ok=True)
    jdk = java_home()
    system = "darwin" if sys.platform == "darwin" else "linux"
    flags = ["-fno-strict-aliasing", "-Wno-write-strings", "-std=c++11", "-O1"]
    linker = [
        f"-L{jdk / 'lib'}",
        "-ljava",
        f"-L{jdk / 'lib/server'}",
        "-ljvm",
        f"-Wl,-rpath,{jdk / 'lib'}",
        f"-Wl,-rpath,{jdk / 'lib/server'}",
    ]
    if system == "darwin":
        flags.append("-mmacosx-version-min=11.0")
        linker.append("-mmacosx-version-min=11.0")
    env = os.environ | {
        "JAVA_HOME": str(jdk),
        "JCC_JDK": str(jdk),
        "PATH": str(jdk / "bin") + os.pathsep + os.environ["PATH"],
        "NO_SHARED": "1",  # One generated JNI extension per process; no shared JCC ABI.
        "JCC_INCLUDES": os.pathsep.join(
            [str(jdk / "include"), str(jdk / "include" / system)]
        ),
        "JCC_CFLAGS": os.pathsep.join(flags),
        "JCC_LFLAGS": os.pathsep.join(linker),
        "JCC_JAVAC": str(jdk / "bin/javac"),
    }
    if system == "darwin":
        env["MACOSX_DEPLOYMENT_TARGET"] = "11.0"
    run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            "setuptools==75.8.2",
            "wheel==0.45.1",
            "build==1.2.2.post1",
        ]
    )
    source = CACHE / f"pylucene-{VERSION}"
    if not (source / "jcc/setup.py").exists():
        archive_path = CACHE / f"pylucene-{VERSION}-src.tar.gz"
        download(
            f"https://dlcdn.apache.org/lucene/pylucene/{archive_path.name}",
            archive_path,
            SOURCE_SHA256,
        )
        with tarfile.open(archive_path) as archive:
            archive.extractall(CACHE, filter="data")
    jcc_wheel = next(
        (p for p in wheels.glob("*.whl") if p.name.lower().startswith("jcc-3.14-")),
        None,
    )
    if jcc_wheel is None:
        run(
            [
                sys.executable,
                "setup.py",
                "build_ext",
                "--parallel",
                "2",
                "bdist_wheel",
                "--dist-dir",
                wheels,
            ],
            cwd=source / "jcc",
            env=env,
        )
        jcc_wheel = next(
            p for p in wheels.glob("*.whl") if p.name.lower().startswith("jcc-3.14-")
        )
    run(["uv", "pip", "install", "--python", sys.executable, "--no-deps", jcc_wheel])
    native_wheel = next(wheels.glob(f"lucene-{VERSION}-*.whl"), None)
    jars, provenance = [], []
    if native_wheel is None:
        jar_root = CACHE / "jars"
        jar_root.mkdir(exist_ok=True)
        for module in MODULES:
            name = f"lucene-{module}-{VERSION}.jar"
            url = f"https://repo.maven.apache.org/maven2/org/apache/lucene/lucene-{module}/{VERSION}/{name}"
            saved_checksum = jar_root / (name + ".sha512")
            if saved_checksum.exists():
                checksum = saved_checksum.read_text().strip()
            else:
                response = requests.get(url + ".sha512", timeout=30)
                response.raise_for_status()
                checksum = response.text.split()[0]
                saved_checksum.write_text(checksum + "\n")
            path = jar_root / name
            download(url, path, checksum, "sha512")
            jars.extend(["--jar", str(path)])
            provenance.append({"url": url, "sha512": checksum})
        generated = CACHE / "generated"
        generated.mkdir(exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "jcc",
            *jars,
            "--use_full_names",
            "--package",
            "java.lang",
            "java.lang.System",
            "java.lang.Runtime",
            "--package",
            "java.util",
            "java.util.HashMap",
            "java.util.Arrays",
            "--package",
            "java.util.function",
            "--package",
            "java.util.regex",
            "--package",
            "java.nio.file",
            "java.nio.file.Paths",
            "java.nio.file.Path",
            "--package",
            "java.io",
            "java.io.StringReader",
            "--exclude",
            "module-info",
            "--python",
            "lucene",
            "--version",
            VERSION,
            "--files",
            "32",
            "--generate",
        ]
        run(command, cwd=generated, env=env)
        run(
            [
                sys.executable,
                "setup.py",
                "build_ext",
                "--parallel",
                "2",
                "bdist_wheel",
                "--dist-dir",
                wheels,
            ],
            cwd=generated,
            env=env,
        )
        native_wheel = next(wheels.glob(f"lucene-{VERSION}-*.whl"))
    run(["uv", "pip", "install", "--python", sys.executable, "--no-deps", native_wheel])
    run(
        [
            sys.executable,
            "-c",
            "import lucene; lucene.initVM(vmargs=['-Djava.awt.headless=true']); from org.apache.lucene.util import Version; print('PyLucene', lucene.VERSION, 'Lucene', Version.LATEST)",
        ],
        env=env,
    )
    (CACHE / "build.json").write_text(
        json.dumps(
            {
                "pylucene_source_sha256": SOURCE_SHA256,
                "pylucene_version": VERSION,
                "jcc_version": "3.14",
                "modules": MODULES,
                "java_home": str(jdk),
                "python": sys.version,
                "platform": platform.platform(),
                "wheel": str(native_wheel),
                "wheel_sha256": digest(native_wheel),
                "jars": provenance,
                "shared_jcc": False,
            },
            indent=2,
        )
        + "\n"
    )
    print(
        "PyLucene installed. uv run preserves these native packages; after an exact uv sync, rerun this installer to reuse the cached wheels."
    )


if __name__ == "__main__":
    main()
