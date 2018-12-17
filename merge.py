#! /usr/bin/env python3

import os
import os.path
import shlex
import codecs
import sqlite3
import fnmatch
import threading
import subprocess
from textwrap import dedent


DD_BLKSIZE = 512
MAX_BLOCKS = 20480
MAX_FILE_SIZE = DD_BLKSIZE*MAX_BLOCKS


ignore = {
    '.DS_Store',
    '.sync*',
    '*Thumbs.db',
}


def is_ignored(rel_fname):
    """Return True if the file should be ignored"""
    for pattern in ignore:
        if fnmatch.fnmatch(rel_fname, pattern):
            return True
    return False


def get_hash(filename):
    "Get the SHA256 hash of the first 10MB of a file"

    if os.stat(filename).st_size < MAX_FILE_SIZE:
        res = subprocess.check_output(['shasum','-a','256',filename])
    else:
        p1 = subprocess.Popen(
            ["dd", f"if={filename}", f"count={MAX_BLOCKS}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        p2 = subprocess.Popen(
            ["shasum", "-a", "256"],
            stdin=p1.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        p1.stdout.close()
        res = p2.communicate()[0]

    return res.split()[0].decode("ASCII")


def make_executable(path):
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2    # copy R bits to X
    os.chmod(path, mode)


def grok_dir(the_dir, db_name, table):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    sql = "INSERT INTO {!s} VALUES ({!r}, {!r}, {!r})"

    for root, dirs, files in os.walk(the_dir):
        for name in files:
            full_name = os.path.join(root, name)
            relpath = os.path.relpath(full_name, start=the_dir)
            if is_ignored(relpath):
                print("Skipping file: {}".format(relpath))
                continue

            file_hash = get_hash(os.path.join(the_dir, relpath))

            print("Read file: %r" % (full_name,))
            cursor.execute(sql.format(table, file_hash, the_dir, relpath))
            conn.commit()


def db_setup(db_file=":memory:"):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS a_files
                 (hash text, start_dir text, relpath text)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS b_files
                 (hash text, start_dir text, relpath text)''')

    conn.commit()

    return conn, cursor


def db_query_missing(c, ignore_paths=[], a_only=False):
    """
    Find files that are missing from one or another directory

    Uses the file hahshes, so isn't fooled by files that are just moved
    Ignores paths in ignore_paths (good for skipping files that changed)

    If a_only supplied, only return the ones missing from directory A
    """
    query = '''SELECT substr(hash, 0, 20) AS short_hash, start_dir, relpath
                    FROM {1}
                    WHERE {1}.hash NOT IN (SELECT hash FROM {0})'''

    if len(ignore_paths) > 0:
        query += " AND {1}.relpath NOT IN ('" + "','".join(ignore_paths) + "')"

    c.execute(query.format("a_files", "b_files"))
    not_in_a = c.fetchall()
    if a_only:
        return not_in_a

    c.execute(query.format("b_files", "a_files"))
    return not_in_a + c.fetchall()


def db_query_moved(c):
    """
    Find files that have just moved
    """
    c.execute('''CREATE TEMP TABLE hashjoin AS
                    SELECT substr(a.hash, 0, 20) AS short_hash, a.relpath AS a_path, b.relpath AS b_path
                    FROM a_files a, b_files b
                    WHERE b.hash = a.hash''')
    # these are paths that have definitely not moved: they have equal hash+path
    # without this check, duplicates in different paths would show up as moved
    c.execute('''CREATE TEMP TABLE excluded AS
                    SELECT a_path FROM hashjoin WHERE a_path = b_path''')
    c.execute('''SELECT short_hash, a_path, b_path
                    FROM hashjoin
                    WHERE a_path != b_path
                    AND a_path NOT IN excluded
                    AND b_path NOT IN excluded''')
    return c.fetchall()


def db_query_duplicates(c, table="a_files"):
    c.execute('''SELECT substr(hash, 0, 20) AS short_hash, start_dir, relpath
                    FROM {table}
                    WHERE hash IN (
                        SELECT hash FROM {table}
                            GROUP BY hash
                            HAVING ( COUNT(hash) > 1 ))
                    ORDER BY short_hash'''.format(table=table))
    return c.fetchall()


def db_query_changed(c):
    "Find files that may be changed or corrupted"
    c.execute('''SELECT substr(a.hash, 0, 20) AS a_short_hash,
                        substr(b.hash, 0, 20) AS b_short_hash, a.relpath AS relpath
                    FROM a_files a, b_files b
                    WHERE b.relpath = a.relpath
                        AND (b.hash != a.hash)''')
    return c.fetchall()


def db_full_report(c, to_a_only=False):
    changed = db_query_changed(c)
    missing = db_query_missing(c, ignore_paths=[path for _,_,path in changed], a_only=to_a_only)
    moved = db_query_moved(c)
    duplicates = db_query_duplicates(c)
    return {"changed":changed, "missing":missing,
        "moved":moved, "duplicates":duplicates}


def populate_new_db(db_name, dir_a, dir_b):
    a_thread = threading.Thread(target=grok_dir,
        args=(dir_a, db_name, "a_files"))
    b_thread = threading.Thread(target=grok_dir,
        args=(dir_b, db_name, "b_files"))
    a_thread.start()
    b_thread.start()
    a_thread.join()
    b_thread.join()


def choose_one(choices):
    """
    Prompt the user to choose among a list of choices

    Takes a list of choices, returns a list of all the ones
    that should be removed
    """
    print("\n\nWhich one should we keep?")
    for n,path in enumerate(choices):
        print("{} - {}".format(n+1, path))
    print("(all - 'a', none - 'n')")
    while True:
        cmd = input("?: ")
        if cmd == 'a':
            return []
        if cmd == 'n':
            return choices
        try:
            idx = int(cmd) - 1
            choices.remove(choices[idx])
            return choices
        except (ValueError, IndexError):
            print("Didn't catch that...")


def create_dedup_script(duplicates):
    """
    Create a script that will eliminate duplicates by asking the
    user on a case-by-case basis which file to keep
    """
    if len(duplicates) < 2:
        print("No duplicates found...")
        return

    script = open("dedup.sh", "wb")
    script.write(codecs.BOM_UTF8)
    script.write(bytes("#! /bin/sh\n", "utf-8"))

    current_hash = duplicates[0][0]
    dupes_for_hash = []
    for short_hash, start_dir, relpath in duplicates:
        if current_hash != short_hash:
            # we've changed hashes - lets ask the user what to do with the list we have
            remove_these = choose_one(dupes_for_hash)
            for fname in remove_these:
                script.write(bytes("rm {}\n".format(shlex.quote(fname)), "utf-8"))

            # reset for the next hash
            dupes_for_hash = []
            current_hash = short_hash

        dupes_for_hash.append(os.path.join(start_dir, relpath))

    script.close()


def create_sync_script(missing_files, top_dirs):
    """
    Create a script that syncs missing files from one location to another

    missing_files is a list of tuples
        (hash, top_dir_the_file_can_be_found_in, relative_path_to_the_file)
    each tuple represents a file that cannot be found in the other top level dir
    """
    if len(missing_files) < 1:
        print("No files missing from either location")
        return

    assert len(top_dirs) == 2, "Please feed 2 top_dirs to create_sync_script()"

    dir_alias = {
        "A": top_dirs[1],
        "B": top_dirs[0],
    }

    script = open("sync.sh", "wb")
    script.write(codecs.BOM_UTF8)

    def write_cmd(cmd):
        script.write(bytes(cmd + "\n", "utf-8"))

    write_cmd(dedent(f"""
        #! /bin/bash
        A={shlex.quote(dir_alias["A"])}
        B={shlex.quote(dir_alias["B"])}
        CMD='cp -v --parents'
        RET_DIR=$(pwd)
        """).lstrip())

    # write the commands that copy files from A to B
    write_cmd('cd "$A"')
    for _,start_dir,relpath in missing_files:
        if start_dir != dir_alias["A"]:
            continue
        write_cmd(f"$CMD {shlex.quote(relpath)} $B")

    # write the commands that copy files from B to A
    write_cmd('cd "$B"')
    for _,start_dir,relpath in missing_files:
        if start_dir != dir_alias["B"]:
            continue
        write_cmd(f"$CMD {shlex.quote(relpath)} $A")

    # close out the script
    write_cmd('cd "$RET_DIR"')
    script.close()
    make_executable("sync.sh")
    print(f"Wrote {len(missing_files)} commands to sync.sh")


def get_dirs(c):
    c.execute('SELECT start_dir FROM a_files LIMIT 1')
    dir_a = c.fetchone()[0]
    c.execute('SELECT start_dir FROM b_files LIMIT 1')
    return [dir_a, c.fetchone()[0]]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="""
            Analyze the contents of two directories to help merge them in the
            presence of additions, deletions, duplicates, and potential movements
            """)
    parser.add_argument("--db", required=True,
        help="Name of the database file to use")
    parser.add_argument("dir_a", nargs="?",
        default='/Users/clark/Documents/src/archive_diff/site_a')
    parser.add_argument("dir_b", nargs="?",
        default='/Users/clark/Documents/src/archive_diff/site_b')
    parser.add_argument("--report", action="store_true",
        help="Report all the differences found")
    parser.add_argument("--dedup", action="store_true",
        help="Create a script to resolve duplicates")
    parser.add_argument("--sync", action="store_true",
        help="Create a script to resolve differences (missing files)")
    parser.add_argument("--consolidate", action="store_true",
        help="Create a script to consolidate files into dir_a")
    args = parser.parse_args()

    was_pre_existing = os.path.isfile(args.db)
    conn, c = db_setup(db_file=args.db)
    if not was_pre_existing:
        populate_new_db(args.db, args.dir_a, args.dir_b)

    report = db_full_report(c, to_a_only=args.consolidate)

    if args.report:
        print("Moved:\n\t" +      "\n\t".join(map(str, report["moved"])))
        print("Changed:\n\t" +    "\n\t".join(map(str, report["changed"])))
        print("Missing:\n\t" +    "\n\t".join(map(str, report["missing"])))
        print("Duplicates:\n\t" + "\n\t".join(map(str, report["duplicates"])))

    if args.dedup:
        create_dedup_script(report['duplicates'])

    if args.sync or args.consolidate:
        create_sync_script(report['missing'], get_dirs(c))

    conn.close()
