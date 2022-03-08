#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

import git

if __name__ == '__main__':

    print("Content-Type: text/html;charset=utf-8", flush=True)
    print("Access-Control-Allow-Origin: *")
    print("Pragma: no-cache", flush=True)
    print("Expires: 0", flush=True)
    print(flush=True)

    dir = Path("/data/stix/end2end/STIXCore")

    os.chdir(dir)
    g = git.cmd.Git(dir)

    p_result = g.pull()

    repo = git.Repo(dir)
    commit = repo.head.commit

    # tagmap = {}
    # for t in repo.tags:
    #   tagmap.setdefault(repo.commit(t), []).append(t)
    #
    # curtag = tagmap[commit]

    tag = "head"

    print(f"Update Project: {p_result}")

    print("delete old files")
    delout = os.popen("rm -rv ./stixcore/data/test/products/end2end/").read()
    print(delout)

    print("Recreate the test files")
    runout = os.popen("./venv/bin/python ./stixcore/util/scripts/end2end_testing.py").read()
    print(runout)

    print("pack and copy test files")
    copyout = os.popen(f"zip -FSrj ../data/{tag}.zip " +
                       "./stixcore/data/test/products/end2end/").read()
    print(copyout)

    print('All end2end rebuilds done!')
