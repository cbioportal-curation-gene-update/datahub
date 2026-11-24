"""Publish study archives from the gene-symbol Datahub fork."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

BUCKET = "cdsi-curation-hgnc"
POINTER = b"version https://git-lfs.github.com/spec/v1"


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def studies():
    result = {}
    for root in ("public", "crdc/gdc"):
        for row in git("ls-tree", f"HEAD:{root}").splitlines():
            attrs, name = row.split("\t", 1)
            _, kind, tree = attrs.split()
            if kind != "tree":
                continue
            if not re.fullmatch(r"[a-zA-Z0-9_.-]+", name) or name in result:
                raise ValueError(f"Unsafe or duplicate study ID: {name}")
            result[name] = (f"{root}/{name}", tree)
    if not result:
        raise ValueError("Empty study inventory")
    return result


def plan(client, inventory):
    def changed(item):
        name, (_, tree) = item
        try:
            obj = client.head_object(Bucket=BUCKET, Key=name + ".tar.gz")
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return True
            raise
        return obj["Metadata"].get("git-tree") != tree

    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = [name for name, stale in zip(inventory, pool.map(changed, inventory.items())) if stale]
    # GitHub supports up to 256 matrix jobs. Keep the seed below that limit.
    size = max(3, (len(pending) + 199) // 200)
    batches = [{"studies": pending[i:i + size]} for i in range(0, len(pending), size)]
    print(f"{len(pending)} of {len(inventory)} studies need publishing")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        print("batches=" + json.dumps(batches), file=output)
        print("revision=" + git("rev-parse", "HEAD"), file=output)


def add_file(archive, file, arcname):
    info = archive.gettarinfo(str(file), arcname)
    if not info.isfile():
        raise ValueError(f"Expected regular study file: {file}")
    with file.open("rb") as source:
        prefix = source.read(len(POINTER))
        source.seek(0)
        if prefix != POINTER:
            archive.addfile(info, source)
            return
        pointer = source.read(1025)
    match = re.fullmatch(re.escape(POINTER) + rb"\noid sha256:([a-f0-9]{64})\nsize (\d+)\n?", pointer)
    if not match:
        raise ValueError(f"Invalid LFS pointer: {file}")
    # Keep one LFS object at a time on disk, including for multi-GB studies.
    with tempfile.TemporaryDirectory() as cache:
        oid = match[1].decode()
        subprocess.run(["git", "-c", f"lfs.storage={cache}", "-c", "lfs.fetchexclude=",
                        "lfs", "smudge", "--", str(file)], input=pointer, stdout=subprocess.DEVNULL,
                       env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "0"}, check=True)
        blob = Path(cache, "objects", oid[:2], oid[2:4], oid)
        info.size = int(match[2])
        with blob.open("rb") as source:
            if blob.stat().st_size != info.size or hashlib.file_digest(source, "sha256").hexdigest() != oid:
                raise ValueError(f"LFS content mismatch: {file}")
            source.seek(0)
            archive.addfile(info, source)


def publish(client, name, path, tree):
    subprocess.run(["git", "sparse-checkout", "set", ".github", path], check=True,
                   env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1"})
    study = Path(path)
    with tempfile.TemporaryDirectory() as tmp:
        filename = Path(tmp, name + ".tar.gz")
        with tarfile.open(filename, "w:gz", compresslevel=1) as archive:
            for file in sorted(study.rglob("*")):
                if file.is_symlink():
                    raise ValueError(f"Unexpected symlink: {file}")
                if file.is_file():
                    add_file(archive, file, str(Path(name) / file.relative_to(study)))
        client.upload_file(str(filename), BUCKET, filename.name, ExtraArgs={
            "ContentType": "application/gzip", "Metadata": {"git-tree": tree, "git-commit": git("rev-parse", "HEAD")}
        })
        print(f"Published {name} ({tree})", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "publish", "index"])
    args = parser.parse_args()
    inventory = studies()
    client = boto3.client("s3")
    if args.mode == "plan":
        plan(client, inventory)
    elif args.mode == "publish":
        for name in json.loads(os.environ["STUDIES"]):
            publish(client, name, *inventory[name])
    else:
        data = (json.dumps(sorted(inventory)) + "\n").encode()
        try:
            existing = client.get_object(Bucket=BUCKET, Key="study_list.json")
            if existing["Body"].read() == data:
                return
        except client.exceptions.NoSuchKey:
            pass
        client.put_object(Bucket=BUCKET, Key="study_list.json", Body=data, ContentType="application/json")


if __name__ == "__main__":
    main()
