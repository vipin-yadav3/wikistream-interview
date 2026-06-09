"""
Setup Fix Verification Tests
==============================
Tests that every issue identified in the manual setup feedback review
has been fixed. Run with:  python3 -m pytest tests/test_setup_fixes.py -v

These tests check file contents, Docker state, and command output.
Docker must be running (make up) for the Docker-dependent tests.
"""

import json
import os
import re
import subprocess
import socket

import pytest
import yaml   # pip install pyyaml


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def read(path):
    return open(os.path.join(REPO_ROOT, path)).read()

def docker_exec(container, *cmd):
    result = subprocess.run(
        ["docker", "exec", container] + list(cmd),
        capture_output=True, text=True, timeout=15
    )
    return result

def port_open(host, port):
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.close()
        return True
    except OSError:
        return False


# ── File-based checks (no Docker needed) ─────────────────────────────────────

class TestDockerComposeFixes:

    def test_issue1_kafka_init_no_wrapping_quotes(self):
        """
        ISSUE #1 (Blocker): kafka-init command wrapped in extra quotes caused
        bash to treat the quoted string as the command name — topics never created.
        Fix: use YAML | block scalar instead of > with "...".
        """
        dc = yaml.safe_load(read("docker-compose.yml"))
        cmd = dc["services"]["kafka-init"]["command"]
        # Must be a plain string (| block scalar), not start/end with a double quote
        assert not str(cmd).strip().startswith('"'), (
            'kafka-init command must NOT be wrapped in double quotes. '
            'Use YAML | block scalar instead of > "...".'
        )
        # Must contain the actual kafka-topics binary call
        assert "kafka-topics" in str(cmd), "kafka-init command must call kafka-topics"
        assert "wiki.recentchanges" in str(cmd), "Must create wiki.recentchanges topic"
        assert "wiki.en-only" in str(cmd), "Must create wiki.en-only topic"

    def test_issue1_kafka_init_restart_on_failure(self):
        """
        ISSUE #1 (follow-up): kafka-init should restart on failure so errors
        are visible in docker compose ps rather than silently swallowed.
        """
        dc = yaml.safe_load(read("docker-compose.yml"))
        restart = dc["services"]["kafka-init"].get("restart", "no")
        assert restart == "on-failure", (
            f"kafka-init restart policy should be 'on-failure', got '{restart}'"
        )

    def test_issue7_no_obsolete_version_key(self):
        """
        ISSUE #7 (Low): version: '3.8' in docker-compose.yml is obsolete
        and causes a Docker warning on every make up.
        """
        dc = yaml.safe_load(read("docker-compose.yml"))
        assert "version" not in dc, (
            "docker-compose.yml must not contain the obsolete 'version' key"
        )

    def test_issue8_minio_init_no_wrapping_quotes(self):
        """
        Same quoting fix applied to minio-init for consistency.
        """
        dc = yaml.safe_load(read("docker-compose.yml"))
        cmd = dc["services"]["minio-init"]["command"]
        assert not str(cmd).strip().startswith('"'), (
            "minio-init command must NOT be wrapped in double quotes"
        )


class TestMakefileFixes:

    def test_issue2_python_not_hardcoded(self):
        """
        ISSUE #2 (Blocker): PYTHON := python3.10 hardcoded the interpreter,
        causing ModuleNotFoundError when pip installed to a different Python.
        Fix: PYTHON ?= python3 (uses candidate's default, overridable).
        """
        makefile = read("Makefile")
        assert "PYTHON := python3.10" not in makefile, (
            "PYTHON must not be hardcoded to python3.10 — use 'PYTHON ?= python3'"
        )
        assert "PYTHON ?= python3" in makefile, (
            "Makefile must use 'PYTHON ?= python3' (with ?= not :=)"
        )

    def test_issue2_setup_uses_python_m_pip(self):
        """
        ISSUE #2/#3: setup target must use 'python3 -m pip' (or $(PYTHON) -m pip)
        not bare 'pip' or 'pip3', so packages go into the correct interpreter.
        """
        makefile = read("Makefile")
        # Find the setup target block
        setup_block = re.search(r'setup:.*?(?=\n[a-zA-Z])', makefile, re.DOTALL)
        assert setup_block, "setup target not found in Makefile"
        block = setup_block.group(0)
        assert "pip install" not in block or "$(PYTHON) -m pip" in block or "python3 -m pip" in block, (
            "setup must use '$(PYTHON) -m pip install' not bare 'pip install'"
        )
        assert "-m pip install" in makefile, (
            "Makefile must use 'python3 -m pip install -r requirements.txt'"
        )

    def test_issue9_pytest_uses_python_m_pytest(self):
        """
        ISSUE #9 (Low): bare 'pytest' fails if ~/.local/bin is not on PATH.
        Fix: use $(PYTHON) -m pytest.
        """
        makefile = read("Makefile")
        # Should not use bare 'pytest' as a command
        assert "PYTEST = $(PYTHON) -m pytest" in makefile or \
               "PYTEST := $(PYTHON) -m pytest" in makefile or \
               "python3 -m pytest" in makefile or \
               "$(PYTHON) -m pytest" in makefile, (
            "Makefile must use '$(PYTHON) -m pytest' not bare 'pytest'"
        )

    def test_check_env_target_exists(self):
        """
        ISSUE #5 (High): No prerequisite validation — candidate hit confusing
        errors with no hint about root cause.
        Fix: make check-env target validates Python, Java, Docker before setup.
        """
        makefile = read("Makefile")
        assert "check-env:" in makefile, "Makefile must have a check-env target"
        assert "java" in makefile.lower() or "Java" in makefile, (
            "check-env must check for Java"
        )
        assert "docker" in makefile.lower() or "Docker" in makefile, (
            "check-env must check for Docker"
        )

    def test_issue4_check_minio_uses_host_docker_internal(self):
        """
        ISSUE #4 (Medium): 'make check-minio' used --network host which doesn't
        work on macOS Docker Desktop. Fix: use host.docker.internal instead.
        """
        makefile = read("Makefile")
        # Find check-minio block
        assert "host.docker.internal" in makefile, (
            "check-minio must use host.docker.internal (not --network host) "
            "for macOS Docker Desktop compatibility"
        )
        assert "--network host" not in makefile, (
            "Must not use --network host — broken on macOS Docker Desktop"
        )

    def test_setup_mentions_jar_download_time(self):
        """
        ISSUE #6 (Low): make setup silently downloads ~500MB JARs, looks like a hang.
        Fix: explicit warning message in setup output.
        """
        makefile = read("Makefile")
        assert "JAR" in makefile or "jar" in makefile.lower(), (
            "setup target must mention JAR download so candidate knows it's not hung"
        )
        assert "min" in makefile.lower() or "minute" in makefile.lower(), (
            "setup target must indicate how long the JAR download takes"
        )


class TestReadmeFixes:

    def test_issue5_prerequisites_section_exists(self):
        """
        ISSUE #5 (High): README had no mention of required Python, Java, or Docker.
        Candidates hit confusing errors with no context.
        Fix: Prerequisites section with version requirements and install links.
        """
        readme = read("README.md")
        assert "Prerequisites" in readme or "prerequisite" in readme.lower(), (
            "README must have a Prerequisites section"
        )
        assert "Python" in readme and "Java" in readme and "Docker" in readme, (
            "Prerequisites must mention Python, Java, and Docker"
        )

    def test_issue3_readme_uses_python_m_pip(self):
        """
        ISSUE #3 (Medium): README said 'pip install' but macOS only has pip3 by default.
        Fix: use 'python3 -m pip install' which works regardless of pip alias.
        """
        readme = read("README.md")
        # Should NOT have bare 'pip install' in setup steps
        lines_with_pip = [l for l in readme.splitlines()
                          if "pip install" in l and not l.strip().startswith("#")]
        bad = [l for l in lines_with_pip
               if "python3 -m pip" not in l and "$(PYTHON)" not in l
               and not l.strip().startswith(">")]
        assert not bad, (
            f"README setup steps must use 'python3 -m pip install', not bare 'pip install'.\n"
            f"Bad lines: {bad}"
        )

    def test_issue8_readme_mentions_confluent_not_bitnami(self):
        """
        ISSUE #8 (Low): README described Kafka as 'bitnami KRaft mode' but
        docker-compose uses Confluent + Zookeeper.
        """
        readme = read("README.md")
        assert "bitnami" not in readme.lower(), (
            "README must not say 'bitnami' — we use Confluent images"
        )
        assert "KRaft" not in readme, (
            "README must not say 'KRaft' — we use Zookeeper mode"
        )

    def test_issue6_readme_warns_about_jar_download(self):
        """
        ISSUE #6 (Low): README gave no warning about the ~500MB JAR download.
        Candidates thought setup had hung.
        """
        readme = read("README.md")
        assert "500" in readme or "JAR" in readme or "jar" in readme.lower(), (
            "README must warn that setup downloads large JARs (~500MB)"
        )
        assert "hang" in readme.lower() or "stall" in readme.lower() or "expected" in readme.lower(), (
            "README must clarify that the JAR download is expected behaviour, not a hang"
        )


# ── Docker-dependent tests (require: make up) ─────────────────────────────────

def docker_running():
    result = subprocess.run(["docker", "ps"], capture_output=True, timeout=5)
    return result.returncode == 0

def kafka_healthy():
    r = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", "wiki-kafka"],
        capture_output=True, text=True, timeout=5
    )
    return r.stdout.strip() == "healthy"


@pytest.mark.skipif(not docker_running(), reason="Docker not running")
class TestDockerRuntime:

    def test_issue1_kafka_topics_created(self):
        """
        ISSUE #1 (Blocker runtime): Both Kafka topics must actually exist.
        Previously the quoting bug silently prevented topic creation.
        """
        if not kafka_healthy():
            pytest.skip("Kafka container not healthy — run: make up")
        result = docker_exec("wiki-kafka", "kafka-topics",
                             "--bootstrap-server", "localhost:9092", "--list")
        topics = result.stdout.strip().splitlines()
        assert "wiki.recentchanges" in topics, (
            f"Topic 'wiki.recentchanges' not found. Topics present: {topics}\n"
            "kafka-init likely failed — check: docker logs wiki-kafka-init"
        )
        assert "wiki.en-only" in topics, (
            f"Topic 'wiki.en-only' not found. Topics present: {topics}"
        )

    def test_postgres_tables_created(self):
        """Postgres tables from init.sql must exist."""
        result = docker_exec(
            "wiki-postgres", "psql", "-U", "wiki", "-d", "wikidb",
            "-c", "SELECT tablename FROM pg_tables WHERE schemaname='public';"
        )
        output = result.stdout
        assert "wiki_edit_counts" in output, "wiki_edit_counts table missing"
        assert "wiki_edit_counts_staging" in output, "staging table missing"
        assert "bot_alerts" in output, "bot_alerts table missing"

    def test_kafka_port_reachable(self):
        """Kafka must be reachable on localhost:9092."""
        assert port_open("localhost", 9092), (
            "Kafka not reachable on localhost:9092 — run: make up"
        )

    def test_postgres_port_reachable(self):
        """Postgres must be reachable on localhost:5432."""
        assert port_open("localhost", 5432), (
            "Postgres not reachable on localhost:5432 — run: make up"
        )

    def test_minio_port_reachable(self):
        """MinIO must be reachable on localhost:9000."""
        assert port_open("localhost", 9000), (
            "MinIO not reachable on localhost:9000 — run: make up"
        )
